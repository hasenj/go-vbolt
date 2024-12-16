package vbolt

import (
	"bytes"
	"math/rand"
	"os"
	"testing"

	"go.hasen.dev/generic"
	"go.hasen.dev/vpack"
)

func TestIndex(t *testing.T) {
	const filename = "_test_db.bolt"
	defer os.Remove(filename)

	db := Open("_test_db.bolt")
	defer db.Close()

	var dbInfo Info

	info := Index(&dbInfo, "idx1", vpack.StringZ, vpack.Int)

	type entry struct {
		term     string
		target   int
		priority uint16
	}

	expectedEntries := []entry{
		{"abc", 12, 2},
		{"lol", 10, 4},
		{"lol", 12, 5},
		{"rofl", 10, 7},
		{"klm", 12, 10},
	}

	expectedCounts := map[string]int{
		"abc":  1,
		"lol":  2,
		"rofl": 1,
		"klm":  1,
	}

	foundEntries := make(map[entry]bool)
	foundCounts := make(map[string]int)

	WithWriteTx(db, func(tx *Tx) {
		SetTargetTerms(tx, info, 10, map[string]uint16{
			"abc": 1,
			"lol": 2,
		})
		SetTargetTerms(tx, info, 12, map[string]uint16{
			"abc": 2,
			"klm": 10,
			"lol": 5,
		})
		tx.Commit()
	})

	WithWriteTx(db, func(tx *Tx) {
		SetTargetTerms(tx, info, 10, map[string]uint16{
			"lol":  4,
			"rofl": 7,
		})
		tx.Commit()
	})

	// verify results

	WithReadTx(db, func(tx *Tx) {
		IterateAllTerms(tx, info, func(term string, target int, priority uint16) bool {
			foundEntries[entry{term, target, priority}] = true
			var count int
			ReadTermCount(tx, info, &term, &count)
			foundCounts[term] = count
			return true
		})
	})

	for _, e := range expectedEntries {
		if !foundEntries[e] {
			t.Logf("Entry not found: %s %d %d", e.term, e.target, e.priority)
			t.Fail()
		}
	}

	for term, count := range expectedCounts {
		if foundCounts[term] != count {
			t.Logf("Entry Count Different. Expected: %d. Found: %d", count, foundCounts[term])
			t.Fail()
		}
	}
	for term, count := range foundCounts {
		var _, ok = expectedCounts[term]
		if !ok {
			t.Logf("Unaccounted for term; no expected counts! Term: %s. Count: %d", term, count)
			t.Fail()
		}
	}

	for entry := range foundEntries {
		if !generic.OneOf(entry, expectedEntries) {
			t.Logf("Found a bogus entry: %s %d %d", entry.term, entry.target, entry.priority)
			t.Fail()
		}
	}

	if len(foundEntries) != len(expectedEntries) {
		t.Logf("Found entries and expected entries don't match! %d != %d", len(foundEntries), len(expectedEntries))
		t.Fail()
	}
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rand.Intn(256))
	}
	return b
}

func TestNextPrefix(t *testing.T) {
	testValues := [][]byte{
		// just some random values
		{},
		{0},
		randomBytes(1), randomBytes(2), randomBytes(2),
		{255},
		{255, 255},
		randomBytes(3), randomBytes(3), randomBytes(3), randomBytes(3),
		randomBytes(8), randomBytes(8), randomBytes(8), randomBytes(8),
		randomBytes(10), randomBytes(10), randomBytes(10),
		{255, 255, 255, 255, 255, 255},
	}

	for _, v := range testValues {
		next := _NextPrefix(v)
		if bytes.Compare(next, v) <= 0 {
			t.Logf("Next prefix of %x <= %x", next, v)
			t.Fail()
		}
	}
}
