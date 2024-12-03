package vbolt

import (
	"log"
	"sync"
	"time"

	"go.hasen.dev/generic"
	"go.hasen.dev/vpack"
)

func TxWriteBatches[Key, Struct any](db *DB, info *BucketInfo[Key, Struct], batchSize int, processFn func(tx *Tx, batch []Struct)) {
	items := make([]Struct, 0, batchSize)
	var nextId Key
	var done bool
	for !done {
		generic.ShrinkTo(&items, 0)
		WithWriteTx(db, func(tx *Tx) {
			nextId, done = ScanList(tx, info, nextId, batchSize, &items)
			processFn(tx, items)
		})
	}
}

// system bucket
var dbInfo Info
var DBProcesses = Bucket(&dbInfo, "proc", vpack.StringZ, vpack.UnixTime)

var _takeTurns sync.Mutex

// kind of like a migration, but mostly we expect it to be about recreating indecies and stuff
func ApplyDBProcess(db *DB, name string, processFn func()) {
	_takeTurns.Lock()
	defer _takeTurns.Unlock()

	shouldRun := false
	WithReadTx(db, func(tx *Tx) {
		var ts time.Time
		shouldRun = !Read(tx, DBProcesses, name, &ts)
	})
	if !shouldRun {
		return
	}

	startTime := time.Now()
	log.Printf("Process: %s :: START", name)
	processFn()
	log.Printf("Process: %s :: END     [%s]", name, time.Since(startTime))
	WithWriteTx(db, func(tx *Tx) {
		ts := time.Now()
		Write(tx, DBProcesses, name, &ts)
		tx.Commit()
	})
}

func RunProcess(label string, processFn func()) {
	_takeTurns.Lock()
	defer _takeTurns.Unlock()

	startTime := time.Now()
	log.Printf("%s :: START", label)
	processFn()
	log.Printf("%s :: END    [%s]", label, time.Since(startTime))
}
