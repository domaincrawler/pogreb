package pogreb

import (
	"bytes"
	"context"
	"math"
	"os"
	"sync"
	"time"

	"github.com/domaincrawler/pogreb/fs"
	"github.com/domaincrawler/pogreb/internal/errors"
	"github.com/domaincrawler/pogreb/internal/hash"
)

const (
	// MaxKeyLength is the maximum size of a key in bytes.
	MaxKeyLength = math.MaxUint16

	// MaxKeys is the maximum numbers of keys in the DB.
	MaxKeys = math.MaxUint32

	metaExt    = ".pmt"
	dbMetaName = "db" + metaExt
)

// DB represents the key-only storage.
// All DB methods are safe for concurrent use by multiple goroutines.
type DB struct {
	mu                sync.RWMutex // Allows multiple database readers or a single writer.
	opts              *Options
	index             *index
	datalog           *datalog
	lock              fs.LockFile // Prevents opening multiple instances of the same database.
	hashSeed          uint32
	metrics           *Metrics
	syncWrites        bool
	cancelBgWorker    context.CancelFunc
	closeWg           sync.WaitGroup
	compactionRunning int32 // Prevents running compactions concurrently.
}

type dbMeta struct {
	HashSeed uint32
}

// Open opens or creates a new DB.
// The DB must be closed after use, by calling Close method.
func Open(path string, opts *Options) (*DB, error) {
	opts = opts.copyWithDefaults(path)

	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	// Try to acquire a file lock.
	lock, acquiredExistingLock, err := createLockFile(opts)
	if err != nil {
		if err == os.ErrExist {
			err = errLocked
		}
		return nil, errors.Wrap(err, "creating lock file")
	}
	clean := lock.Unlock
	defer func() {
		if clean != nil {
			_ = clean()
		}
	}()

	if acquiredExistingLock {
		// Lock file already existed, but the process managed to acquire it.
		// It means the database wasn't closed properly.
		// Start recovery process.
		if err := backupNonsegmentFiles(opts.FileSystem); err != nil {
			return nil, err
		}
	}

	index, err := openIndex(opts)
	if err != nil {
		return nil, errors.Wrap(err, "opening index")
	}

	datalog, err := openDatalog(opts)
	if err != nil {
		return nil, errors.Wrap(err, "opening datalog")
	}

	db := &DB{
		opts:       opts,
		index:      index,
		datalog:    datalog,
		lock:       lock,
		metrics:    &Metrics{},
		syncWrites: opts.BackgroundSyncInterval == -1,
	}
	if index.count() == 0 {
		// The index is empty, make a new hash seed.
		seed, err := hash.RandSeed()
		if err != nil {
			return nil, err
		}
		db.hashSeed = seed
	} else {
		if err := db.readMeta(); err != nil {
			return nil, errors.Wrap(err, "reading db meta")
		}
	}

	if acquiredExistingLock {
		if err := db.recover(); err != nil {
			return nil, errors.Wrap(err, "recovering")
		}
	}

	if db.opts.BackgroundSyncInterval > 0 || db.opts.BackgroundCompactionInterval > 0 {
		db.startBackgroundWorker()
	}

	clean = nil
	return db, nil
}

func cloneBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func (db *DB) writeMeta() error {
	m := dbMeta{
		HashSeed: db.hashSeed,
	}
	return writeGobFile(db.opts.FileSystem, dbMetaName, m)
}

func (db *DB) readMeta() error {
	m := dbMeta{}
	if err := readGobFile(db.opts.FileSystem, dbMetaName, &m); err != nil {
		return err
	}
	db.hashSeed = m.HashSeed
	return nil
}

func (db *DB) hash(data []byte) uint32 {
	return hash.Sum32WithSeed(data, db.hashSeed)
}

// newNullableTicker is a wrapper around time.NewTicker that allows creating a nil ticker.
// A nil ticker never ticks.
func newNullableTicker(d time.Duration) (<-chan time.Time, func()) {
	if d > 0 {
		t := time.NewTicker(d)
		return t.C, t.Stop
	}
	return nil, func() {}
}

func (db *DB) startBackgroundWorker() {
	ctx, cancel := context.WithCancel(context.Background())
	db.cancelBgWorker = cancel
	db.closeWg.Add(1)

	go func() {
		defer db.closeWg.Done()

		syncC, syncStop := newNullableTicker(db.opts.BackgroundSyncInterval)
		defer syncStop()

		compactC, compactStop := newNullableTicker(db.opts.BackgroundCompactionInterval)
		defer compactStop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-syncC:
				if err := db.Sync(); err != nil {
					logger.Printf("error synchronizing database: %v", err)
				}
			case <-compactC:
				if cr, err := db.Compact(); err != nil {
					logger.Printf("error compacting database: %v", err)
				} else if cr.CompactedSegments > 0 {
					logger.Printf("compacted database: %+v", cr)
				}
			}
		}
	}()
}

// Has returns true if the DB contains the given key.
func (db *DB) Has(key []byte) (bool, error) {
	h := db.hash(key)
	found := false
	db.mu.RLock()
	defer db.mu.RUnlock()
	err := db.index.get(h, func(sl slot) (bool, error) {
		if uint16(len(key)) != sl.keySize {
			return false, nil
		}
		slKey, err := db.datalog.readKey(sl)
		if err != nil {
			return true, err
		}
		if bytes.Equal(key, slKey) {
			found = true
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return false, err
	}
	return found, nil
}

func (db *DB) put(sl slot, key []byte) error {
	return db.index.put(sl, func(cursl slot) (bool, error) {
		if uint16(len(key)) != cursl.keySize {
			return false, nil
		}
		slKey, err := db.datalog.readKey(cursl)
		if err != nil {
			return true, err
		}
		if bytes.Equal(key, slKey) {
			return true, nil
		}
		return false, nil
	})
}

func (db *DB) HasOrPut(key []byte) (bool, error) {
	if len(key) > MaxKeyLength {
		return false, errKeyTooLarge
	}
	found := false
	h := db.hash(key)
	db.mu.Lock()
	defer db.mu.Unlock()
	err := db.index.get(h, func(sl slot) (bool, error) {
		if uint16(len(key)) != sl.keySize {
			return false, nil
		}
		slKey, err := db.datalog.readKey(sl)
		if err != nil {
			return true, err
		}
		if bytes.Equal(key, slKey) {
			found = true
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return false, err
	}
	if !found {
		segID, offset, err := db.datalog.put(key)
		if err != nil {
			return false, err
		}
		sl := slot{
			hash:      h,
			segmentID: segID,
			keySize:   uint16(len(key)),
			offset:    offset,
		}

		if err := db.put(sl, key); err != nil {
			return false, err
		}

		if db.syncWrites {
			return found, db.sync()
		}
	}
	return found, nil
}

// Put sets the value for the given key. It updates the value for the existing key.
func (db *DB) Put(key []byte) error {
	if len(key) > MaxKeyLength {
		return errKeyTooLarge
	}
	h := db.hash(key)
	db.metrics.Puts.Add(1)
	db.mu.Lock()
	defer db.mu.Unlock()

	segID, offset, err := db.datalog.put(key)
	if err != nil {
		return err
	}

	sl := slot{
		hash:      h,
		segmentID: segID,
		keySize:   uint16(len(key)),
		offset:    offset,
	}

	if err := db.put(sl, key); err != nil {
		return err
	}

	if db.syncWrites {
		return db.sync()
	}
	return nil
}

// Close closes the DB.
func (db *DB) Close() error {
	if db.cancelBgWorker != nil {
		db.cancelBgWorker()
	}
	db.closeWg.Wait()
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.writeMeta(); err != nil {
		return err
	}
	if err := db.datalog.close(); err != nil {
		return err
	}
	if err := db.index.close(); err != nil {
		return err
	}
	if err := db.lock.Unlock(); err != nil {
		return err
	}
	return nil
}

func (db *DB) sync() error {
	return db.datalog.sync()
}

// Items returns a new ItemIterator.
func (db *DB) Items() *ItemIterator {
	return &ItemIterator{db: db}
}

// Sync commits the contents of the database to the backing FileSystem.
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.sync()
}

// Count returns the number of keys in the DB.
func (db *DB) Count() uint32 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.index.count()
}

// Metrics returns the DB metrics.
func (db *DB) Metrics() *Metrics {
	return db.metrics
}

// FileSize returns the total size of the disk storage used by the DB.
func (db *DB) FileSize() (int64, error) {
	var size int64
	files, err := db.opts.FileSystem.ReadDir(".")
	if err != nil {
		return 0, err
	}
	for _, file := range files {
		size += file.Size()
	}
	return size, nil
}
