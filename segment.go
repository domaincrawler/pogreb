package pogreb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	segmentExt = ".psg"
)

// segment is a write-ahead log segment.
// It consists of a sequence of binary-encoded variable length records.
type segment struct {
	*file
	id         uint16 // Physical segment identifier.
	sequenceID uint64 // Logical monotonically increasing segment identifier.
	name       string
	meta       *segmentMeta
}

func segmentName(id uint16, sequenceID uint64) string {
	return fmt.Sprintf("%05d-%d%s", id, sequenceID, segmentExt)
}

type segmentMeta struct {
	Full          bool
	PutRecords    uint32
	DeleteRecords uint32
	DeletedKeys   uint32
	DeletedBytes  uint32
}

func segmentMetaName(id uint16, sequenceID uint64) string {
	return segmentName(id, sequenceID) + metaExt
}

// Binary representation of a segment record:
// +---------------+------------------+------------------+
// | Key Size (2B) | Key              |         CRC (4B) |
// +---------------+------------------+------------------+
type record struct {
	segmentID uint16
	offset    uint32
	data      []byte
	key       []byte
}

func encodedRecordSize(kvSize uint32) uint32 {
	// key size, key, crc32
	return 2 + kvSize + 4
}

func encodePutRecord(key []byte) []byte {
	size := encodedRecordSize(uint32(len(key)))
	data := make([]byte, size)
	binary.LittleEndian.PutUint16(data[:2], uint16(len(key)))
	copy(data[2:], key)
	checksum := crc32.ChecksumIEEE(data[:2+len(key)])
	binary.LittleEndian.PutUint32(data[size-4:size], checksum)
	return data
}

// segmentIterator iterates over segment records.
type segmentIterator struct {
	f      *segment
	offset uint32
	r      *bufio.Reader
	buf    []byte // kv size and crc32 reusable buffer.
}

func newSegmentIterator(f *segment) (*segmentIterator, error) {
	if _, err := f.Seek(int64(headerSize), io.SeekStart); err != nil {
		return nil, err
	}
	return &segmentIterator{
		f:      f,
		offset: headerSize,
		r:      bufio.NewReader(f),
		buf:    make([]byte, 6),
	}, nil
}

func (it *segmentIterator) next() (record, error) {
	// Read key and value size.
	kvSizeBuf := it.buf
	if _, err := io.ReadFull(it.r, kvSizeBuf); err != nil {
		if err == io.EOF {
			return record{}, ErrIterationDone
		}
		return record{}, err
	}

	// Decode key size.
	keySize := uint32(binary.LittleEndian.Uint16(kvSizeBuf[:2]))

	//// Decode value size and record type.
	//valueSize := binary.LittleEndian.Uint32(kvSizeBuf[2:])
	//if valueSize&(1<<31) != 0 {
	//	valueSize &^= 1 << 31
	//}

	// Read key, value and checksum.
	recordSize := encodedRecordSize(keySize)
	data := make([]byte, recordSize)
	copy(data, kvSizeBuf)
	if _, err := io.ReadFull(it.r, data[2:]); err != nil {
		return record{}, err
	}

	// Verify checksum.
	checksum := binary.LittleEndian.Uint32(data[len(data)-4:])
	if checksum != crc32.ChecksumIEEE(data[:len(data)-4]) {
		return record{}, errCorrupted
	}

	offset := it.offset
	it.offset += recordSize
	rec := record{
		segmentID: it.f.id,
		offset:    offset,
		data:      data,
		key:       data[2 : 2+keySize],
	}
	return rec, nil
}
