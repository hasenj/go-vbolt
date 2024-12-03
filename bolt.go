package vbolt

import (
	"log"
	"time"

	"github.com/boltdb/bolt"
	"go.hasen.dev/generic"
)

type DB = bolt.DB
type Tx = bolt.Tx
type BBucket = bolt.Bucket
type Cursor = bolt.Cursor

func Open(filename string) *DB {
	var options bolt.Options
	options.Timeout = time.Second
	options.InitialMmapSize = 1024 * 1024 * 1024
	return generic.Must(bolt.Open(filename, 0644, &options))
}

func ReadTx(db *DB) *Tx {
	if db == nil {
		return nil
	}
	return generic.Must(db.Begin(false))
}

func WriteTx(db *DB) *Tx {
	if db == nil {
		return nil
	}
	return generic.Must(db.Begin(true))
}

func TxClose(tx *Tx) {
	if tx == nil {
		return
	}
	tx.Rollback()
}

func TxRawBucket(tx *Tx, name string) *BBucket {
	bname := generic.UnsafeStringBytes(name)
	bkt := tx.Bucket(bname)
	if bkt == nil && tx.Writable() {
		bkt = generic.Must(tx.CreateBucket(bname))
	}
	return bkt
}

func WithReadTx(db *DB, fn func(tx *Tx)) {
	tx := ReadTx(db)
	defer TxClose(tx)
	fn(tx)
}

func TxCommit(tx *Tx) {
	if tx == nil {
		return
	}
	tx.Commit()
}

// WithWriteTx calls supplied function with a writeable transaction
//
// Caller must commit the tx explicitly; otherwise it will get rolled back by default
func WithWriteTx(db *DB, fn func(tx *Tx)) {
	tx := WriteTx(db)
	defer TxClose(tx)
	fn(tx)
}

type Info struct {
	BucketList []string
	IndexList  []string
	CollectionList []string

	Infos map[string]any
}

func EnsureBuckets(tx *Tx, dbInfo *Info) {
	generic.MustTrue(tx.Writable(), bolt.ErrTxNotWritable)
	for _, name := range dbInfo.BucketList {
		TxRawBucket(tx, name)
	}
	for _, name := range dbInfo.IndexList {
		TxRawBucket(tx, name)
	}
	for _, name := range dbInfo.CollectionList {
		TxRawBucket(tx, name)
	}
}

// Some helpers that most apps will need
func WarmTheCache(tx *Tx, dbInfo *Info) {
	// TODO: re-enable the profiler
	// p.Start(string(bucketName))
	// defer p.Stop()
	readAll := func(name []byte, b *bolt.Bucket) error {
		log.Println("preloading", string(name))
		// we don't have nested bucket so we don't need to worry about them
		b.ForEach(func(k, v []byte) error {
			return nil
		})
		return nil
	}
	tx.ForEach(readAll)
}
