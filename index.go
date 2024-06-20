package vbolt

import (
	"bytes"

	"go.hasen.dev/generic"
	"go.hasen.dev/vpack"
)

/*
	Terminology:

	What we are calling "Index" here is actually a bidirectional multimap that allows mapping "terms" to a "target"

	In general, it's expected that each target would have "few" terms, while a "term" may point to many mant "targets".

	The generic type for "target" is K

	The generic type for  "term"  is T

	The order of parameters is always [K, T]. That is, the target first, then the term.

	Each term->target pairing has a priority to help the calling code "sort" through the matches.
	If you don't care about the priorities, you can just set to zero or provide a nil where a slice is expected

	For example, the numeric ids of words that have a specific kana writing.

	The index api is designed such that:

		- You can add/remove individual (term, target) pairs
		- You can "set" terms for a target once - and the system will automatically add/remove term/target pairs as needed
		- You can "iterate" all the targets matching a term
		- You can "iterate" all the terms matching a target

	The order for arguments is always K, T
*/

const IndexTermPrefix byte = 0x01
const IndexTargetPrefix byte = 0x02
const IndexCountPrefix byte = 0x03

type IndexInfo[K, T comparable] struct {
	Name     string
	TargetFn vpack.SerializeFn[K]
	TermFn   vpack.SerializeFn[T]
}

func Index[K, T comparable](dbInfo *Info, name string, termFn vpack.SerializeFn[T], targetFn vpack.SerializeFn[K]) *IndexInfo[K, T] {
	generic.Append(&dbInfo.IndexList, name)
	return &IndexInfo[K, T]{
		Name:     name,
		TargetFn: targetFn,
		TermFn:   termFn,
	}
}

func termKeyPrefix[K, T comparable](info *IndexInfo[K, T], term *T) []byte {
	buf := vpack.NewWriter()
	buf.WriteBytes(IndexTermPrefix)
	info.TermFn(term, buf)
	return buf.Data
}

func targetKeyPrefix[K, T comparable](info *IndexInfo[K, T], target *K) []byte {
	buf := vpack.NewWriter()
	buf.WriteBytes(IndexTargetPrefix)
	info.TargetFn(target, buf)
	return buf.Data
}

func termTargetKey[K, T comparable](info *IndexInfo[K, T], target *K, term *T, priority *uint16) []byte {
	buf := vpack.NewWriter()
	buf.WriteBytes(IndexTermPrefix)
	info.TermFn(term, buf)
	vpack.FUInt16(priority, buf)
	info.TargetFn(target, buf)
	return buf.Data
}

func termCountKey[K, T comparable](info *IndexInfo[K, T], term *T) []byte {
	buf := vpack.NewWriter()
	buf.WriteBytes(IndexCountPrefix)
	info.TermFn(term, buf)
	return buf.Data
}

func readTargetTerm[K, T comparable](info *IndexInfo[K, T], data []byte) (target K, term T) {
	buf := vpack.NewReader(data)
	buf.Pos++ // skip the IndexRevsrefix byte
	info.TargetFn(&target, buf)
	info.TermFn(&term, buf)
	return
}

func targetTermKey[K, T comparable](info *IndexInfo[K, T], target *K, term *T) []byte {
	buf := vpack.NewWriter()
	buf.WriteBytes(IndexTargetPrefix)
	info.TargetFn(target, buf)
	info.TermFn(term, buf)
	return buf.Data
}

var SerializeCountFn = vpack.Int

func incTermCount[K, T comparable](tx *Tx, info *IndexInfo[K, T], term *T, increment int) {
	key := termCountKey(info, term)
	bkt := TxRawBucket(tx, info.Name)
	v := bkt.Get(key)
	var count int
	vpack.FromBytesInto(v, &count, SerializeCountFn)
	count += increment
	RawMustPut(bkt, key, vpack.ToBytes(&count, SerializeCountFn))
}

func ReadTermCount[K, T comparable](tx *Tx, info *IndexInfo[K, T], term *T, count *int) bool {
	key := termCountKey(info, term)
	bkt := TxRawBucket(tx, info.Name)
	v := bkt.Get(key)
	return vpack.FromBytesInto(v, count, SerializeCountFn)
}

func addTargetTermPair[K, T comparable](tx *Tx, info *IndexInfo[K, T], target *K, term *T, priority *uint16) {
	val := vpack.ToBytes(priority, vpack.FUInt16)
	bkt := TxRawBucket(tx, info.Name)
	bkt.Put(termTargetKey(info, target, term, priority), nil)
	bkt.Put(targetTermKey(info, target, term), val)
}

func delTargetTermPair[K, T comparable](tx *Tx, info *IndexInfo[K, T], target *K, term *T, priority *uint16) {
	targetTermKey := targetTermKey(info, target, term)
	bkt := TxRawBucket(tx, info.Name)
	bkt.Delete(termTargetKey(info, target, term, priority))
	bkt.Delete(targetTermKey)
}

func PlainTerms[T comparable](terms []T) map[T]uint16 {
	return UniformTerms(terms, 0)
}

func UniformTerms[T comparable](terms []T, priority uint16) (out map[T]uint16) {
	generic.InitMap(&out)
	for _, t := range terms {
		out[t] = priority
	}
	return
}

func SetTargetSingleTerm[K, T comparable](tx *Tx, info *IndexInfo[K, T], target K, term T) {
	SetTargetTerms(tx, info, target, PlainTerms([]T{term}))
}

func DeleteTargetTerms[K, T comparable](tx *Tx, info *IndexInfo[K, T], target K) {
	SetTargetTerms(tx, info, target, nil)
}

// sets terms without priorities
func SetTargetTermsPlain[K, T comparable](tx *Tx, info *IndexInfo[K, T], target K, terms []T) {
	SetTargetTerms(tx, info, target, PlainTerms(terms))
}

func SetTargetTermsUniform[K, T comparable](tx *Tx, info *IndexInfo[K, T], target K, terms []T, priority uint16) {
	SetTargetTerms(tx, info, target, UniformTerms(terms, priority))
}

// Updates target,term pairs so that only the terms provided here point to target.
// terms map the term to the priority
func SetTargetTerms[K, T comparable](tx *Tx, info *IndexInfo[K, T], target K, terms map[T]uint16) {
	var existing = make(map[T]uint16)

	// read out the list of existing index terms so we can get the list of actual bucket keys to add / remove
	IterateTarget(tx, info, target, func(term T, priority uint16) bool {
		existing[term] = priority
		return true
	})

	var add = make(map[T]uint16)
	var del = make(map[T]uint16)

	for e, priority := range existing {
		newPriority, isRequested := terms[e]
		if !isRequested || priority != newPriority {
			del[e] = priority // deleting the old priority!
		}
	}

	for t, newPriority := range terms {
		priority, exists := existing[t]
		if !exists || priority != newPriority {
			add[t] = newPriority
		}
	}

	for term, priority := range del {
		delTargetTermPair(tx, info, &target, &term, &priority)
		incTermCount(tx, info, &term, -1)
	}

	for term, priority := range add {
		addTargetTermPair(tx, info, &target, &term, &priority)
		incTermCount(tx, info, &term, 1)
	}
}

func IterateTerm[K, T comparable](tx *Tx, info *IndexInfo[K, T], term T, visitFn func(target K, priority uint16) bool) []byte {
	return _IterateTermCore(tx, info, term, Window{}, visitFn)
}

func IterateTermOffset[K, T comparable](tx *Tx, info *IndexInfo[K, T], term T, offset int, visitFn func(target K, priority uint16) bool) []byte {
	options := Window{Offset: offset}
	return _IterateTermCore(tx, info, term, options, visitFn)
}

func ReadTermTargets[K, T comparable](tx *Tx, info *IndexInfo[K, T], term T, targets *[]K, window Window) []byte {
	return _IterateTermCore(tx, info, term, window, func(target K, priority uint16) bool {
		generic.Append(targets, target)
		return true
	})
}

func ReadTermTargetSingle[K, T comparable](tx *Tx, info *IndexInfo[K, T], term T, target *K) bool {
	var targets []K
	var opts Window
	opts.Limit = 1
	ReadTermTargets(tx, info, term, &targets, opts)
	if len(targets) > 0 {
		*target = targets[0]
		return true
	} else {
		return false
	}

}

type Window struct {
	StartByte []byte
	Offset    int
	Limit     int
}

// iterate over targets that are assigned to term
func _IterateTermCore[K, T comparable](tx *Tx, info *IndexInfo[K, T], term T, window Window, visitFn func(target K, priority uint16) bool) []byte {
	keyPrefix := termKeyPrefix(info, &term)

	if window.StartByte != nil {
		if bytes.HasPrefix(window.StartByte, keyPrefix) {
			keyPrefix = window.StartByte
		} else {
			return nil
		}
	}

	bkt := TxRawBucket(tx, info.Name)

	count := 0
	loaded := 0
	return RawIterateKeyPrefixData(bkt.Cursor(), keyPrefix, func(key []byte, v []byte) bool {
		if count < window.Offset {
			count++
			return true
		}
		// we can safely assume the key starts with IndexTermPrefix because otherwise the RawIterateKeyPrefixValues func will not call us
		term, target, priority := readTermTargetPriority(info, key)
		loaded++
		_ = term
		cont := visitFn(target, priority)
		if window.Limit > 0 && loaded > window.Limit {
			return false
		}
		return cont
	})
}

// iterate over terms that are assigned to target
func IterateTarget[K, T comparable](tx *Tx, info *IndexInfo[K, T], target K, visitFn func(term T, priority uint16) bool) {
	keyPrefix := targetKeyPrefix(info, &target)
	bkt := TxRawBucket(tx, info.Name)

	RawIterateKeyPrefixData(bkt.Cursor(), keyPrefix, func(key []byte, v []byte) bool {
		// we can safely assume the key starts with IndexTermPrefix because otherwise the RawIterateKeyPrefixValues func will not call us
		target, term := readTargetTerm(info, key)
		var priority uint16
		vpack.FromBytesInto(v, &priority, vpack.FUInt16)
		_ = target
		return visitFn(term, priority)
	})
}

func readTermTargetPriority[K, T comparable](info *IndexInfo[K, T], data []byte) (term T, target K, priority uint16) {
	buf := vpack.NewReader(data)
	buf.Pos++ // skip the IndexTermPrefix byte
	info.TermFn(&term, buf)
	vpack.FUInt16(&priority, buf)
	info.TargetFn(&target, buf)
	return
}

func IterateAllTerms[K, T comparable](tx *Tx, info *IndexInfo[K, T], visitFn func(term T, target K, priority uint16) bool) {
	var keyPrefix = []byte{IndexTermPrefix}
	bkt := TxRawBucket(tx, info.Name)

	RawIterateKeyPrefixData(bkt.Cursor(), keyPrefix, func(key []byte, v []byte) bool {
		term, target, priority := readTermTargetPriority(info, key)
		return visitFn(term, target, priority)
	})
}
