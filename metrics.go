package pogreb

import "expvar"

// Metrics holds the DB metrics.
type Metrics struct {
	Puts           expvar.Int
}
