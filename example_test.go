package pogreb_test

import (
	"log"

	"github.com/domaincrawler/pogreb"
)

func Example() {
	db, err := pogreb.Open("pogreb.test", nil)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer db.Close()

	// Insert a new key-value pair.
	if err := db.Put([]byte("testKey")); err != nil {
		log.Fatal(err)
	}

	// Retrieve the inserted value.
	val, err := db.Has([]byte("testKey"))
	if err != nil {
		log.Fatal(err)
	}
	log.Println(val)

	// Iterate over items.
	it := db.Items()
	for {
		key, err := it.Next()
		if err == pogreb.ErrIterationDone {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("%s", key)
	}
}
