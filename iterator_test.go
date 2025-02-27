package pogreb

import (
	"testing"

	"github.com/domaincrawler/pogreb/internal/assert"
)

func TestIteratorEmpty(t *testing.T) {
	db, err := createTestDB(nil)
	assert.Nil(t, err)
	it := db.Items()
	for i := 0; i < 8; i++ {
		_, err := it.Next()
		if err != ErrIterationDone {
			t.Fatalf("expected %v; got %v", ErrIterationDone, err)
		}
	}
	assert.Nil(t, db.Close())
}

func TestIterator(t *testing.T) {
	db, err := createTestDB(nil)
	assert.Nil(t, err)

	items := map[byte]bool{}
	var i byte
	for i = 0; i < 255; i++ {
		items[i] = false
		err := db.Put([]byte{i})
		assert.Nil(t, err)
	}

	it := db.Items()
	for {
		key, err := it.Next()
		if err == ErrIterationDone {
			break
		}
		assert.Nil(t, err)
		if k, ok := items[key[0]]; !ok {
			t.Fatalf("unknown key %v", k)
		}
		items[key[0]] = true
	}

	for k, v := range items {
		if !v {
			t.Fatalf("expected to iterate over key %v", k)
		}
	}

	for i := 0; i < 8; i++ {
		_, err := it.Next()
		if err != ErrIterationDone {
			t.Fatalf("expected %v; got %v", ErrIterationDone, err)
		}
	}

	assert.Nil(t, db.Close())
}
