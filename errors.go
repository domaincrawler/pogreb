package pogreb

import (
	"github.com/akrylysov/pogreb/internal/errors"
)

var (
	errKeyTooLarge   = errors.New("key is too large")
	errFull          = errors.New("database is full")
	errCorrupted     = errors.New("database is corrupted")
	errLocked        = errors.New("database is locked")
	errBusy          = errors.New("database is busy")
)
