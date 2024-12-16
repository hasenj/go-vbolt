package vbolt

import (
	"bytes"

	"go.hasen.dev/generic"
	"go.hasen.dev/vpack"
)

// Core helper functions that buckets and indecies are built on top of

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

type IterationDirection uint8

const IterateRegular = IterationDirection(0)
const IterateReverse = IterationDirection(1)

func _CursorStartPos(c *Cursor, direction IterationDirection) (k []byte, v []byte) {
	if direction == IterateRegular {
		return c.First()
	}
	if direction == IterateReverse {
		return c.Last()
	}
	return
}

func _CursorStartPosForPrefix(c *Cursor, prefix []byte, direction IterationDirection) (k []byte, v []byte) {
	if len(prefix) == 0 {
		return _CursorStartPos(c, direction)
	}
	if direction == IterateRegular {
		return c.Seek(prefix)
	}
	if direction == IterateReverse {
		// find the last item that could have this prefix
		next := _NextPrefix(prefix)
		c.Seek(next)
		return c.Prev()
	}
	return
}

func _NextPrefix(b []byte) []byte {
	// find the index of the last byte that is < 0xff
	var i = len(b) - 1
	for ; i >= 0 && b[i] == 255; i-- {
	}

	if i >= 0 {
		next := bytes.Clone(b)
		next[i] += 1
		return next
	} else {
		// if all bytes are 0xff (or empty buffer), we need to add a new byte
		return append(b, 0)
	}
}

func _CursorStep(c *Cursor, direction IterationDirection) (k []byte, v []byte) {
	if direction == IterateRegular {
		return c.Next()
	}
	if direction == IterateReverse {
		return c.Prev()
	}
	return
}

type _RawIterationParams struct {
	Prefix []byte
	Window
}

// _RawIterateCore is the core function that iterates over a bucket and calls the visitFn for each key/value pair
// returns the "next" key (if any) that would have been visited had the visitor not returned false
// returns nil if the visitor exhausted all the keys that have the given prefix
func _RawIterateCore(bkt *BBucket, window _RawIterationParams, visitFn func(key []byte, value []byte) bool) []byte {
	crsr := bkt.Cursor()
	start := window.Prefix
	if len(window.Cursor) > 0 {
		start = window.Cursor
	}
	key, value := _CursorStartPosForPrefix(crsr, start, window.Direction)

	if window.Offset > 0 {
		for i := 0; i < window.Offset; i++ {
			key, value = _CursorStep(crsr, window.Direction)
			if key == nil {
				return nil
			}
		}
	}

	count := 0
	for key != nil && bytes.HasPrefix(key, window.Prefix) {
		if !visitFn(key, value) {
			break
		}
		count++
		if window.Limit > 0 && count >= window.Limit {
			break
		}
		key, value = _CursorStep(crsr, window.Direction)
	}

	// returns the next key that should be visited to continue the iteration
	if key != nil {
		key, _ = _CursorStep(crsr, window.Direction)
	}
	return key
}
