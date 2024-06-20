package vbolt

import (
	"bytes"

	"go.hasen.dev/generic"
	"go.hasen.dev/vpack"
)

// Core helper functions that buckets and indecies are built on top of

// Put a new entry, returning false if the key already exists in bucket
func RawPutNew(bkt *BBucket, key []byte, value []byte) bool {
	if RawHasKey(bkt, key) {
		return false
	}
	RawMustPut(bkt, key, value)
	return true
}

func RawHasKey(bkt *BBucket, key []byte) bool {
	if bkt == nil {
		return false
	}
	c := bkt.Cursor()
	k, _ := c.Seek(key)
	return bytes.Equal(key, k)
}

// Put an entry
func RawMustPut(bkt *BBucket, key []byte, value []byte) {
	generic.MustOK(bkt.Put(key, value))
}

func RawNextSequence(bucket *BBucket) uint64 {
	return generic.Must(bucket.NextSequence())
}

func RawSetSequenceCorrectly(bucket *BBucket) {
	c := bucket.Cursor()
	lastKeyBytes, _ := c.Last()
	seq := vpack.FromBytes(lastKeyBytes, vpack.FUInt64)
	bucket.SetSequence(*seq)
}

// returns the "next" key (if any) that would have been visited had the visitor not returned false
// returns nil if the visitor exhausted all the keys that have the given prefix
func RawIterateKeyPrefixData(crsr *Cursor, keyPrefix []byte, visitFn func(key []byte, value []byte) bool) []byte {
	key, value := crsr.Seek(keyPrefix)
	for key != nil && bytes.HasPrefix(key, keyPrefix) {
		cont := visitFn(key, value)
		key, value = crsr.Next()
		if !cont {
			return generic.Clone(key)
		}
	}
	return nil
}
