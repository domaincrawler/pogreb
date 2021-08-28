<p align="center"><img src="https://akrylysov.github.io/pogreb/logo.svg" width="300"></p>

# Pogreb
[![Docs](https://godoc.org/github.com/domaincrawler/pogreb?status.svg)](https://pkg.go.dev/github.com/domaincrawler/pogreb)
[![Go Report Card](https://goreportcard.com/badge/github.com/domaincrawler/pogreb)](https://goreportcard.com/report/github.com/domaincrawler/pogreb)
[![Codecov](https://codecov.io/gh/akrylysov/pogreb/branch/master/graph/badge.svg)](https://codecov.io/gh/akrylysov/pogreb)

Pogreb is an embedded key-only store for read-heavy workloads written in Go.

## Key characteristics

- 100% Go.
- Optimized for fast random lookups and infrequent bulk inserts.
- Can store larger-than-memory data sets.
- Low memory usage.
- All DB methods are safe for concurrent use by multiple goroutines.

## Installation

```sh
$ go get -u github.com/domaincrawler/pogreb
```

## Usage

### Opening a database

To open or create a new database, use the `pogreb.Open()` function:

```go
package main

import (
	"log"

	"github.com/domaincrawler/pogreb"
)

func main() {
    db, err := pogreb.Open("pogreb.test", nil)
    if err != nil {
        log.Fatal(err)
        return
    }	
    defer db.Close()
}
```

### Writing to a database

Use the `DB.Put()` function to insert a new key:

```go
err := db.Put([]byte("testKey"))
if err != nil {
	log.Fatal(err)
}
```

### Reading from a database

To retrieve the inserted value, use the `DB.Has()` function:

```go
val, err := db.Has([]byte("testKey"))
if err != nil {
	log.Fatal(err)
}
log.Println(val)
```

### Iterating over items

To iterate over items, use `ItemIterator` returned by `DB.Items()`:

```go
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
```
