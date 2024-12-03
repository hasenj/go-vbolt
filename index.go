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

type IndexInfo[K, T, P comparable] struct {
	Name     string
	TargetPackFn vpack.PackFn[K]
	TermPackFn   vpack.PackFn[T]
	PriorityPackFn vpack.PackFn[P]
}

func Index[K, T comparable](dbInfo *Info, name string, termFn vpack.PackFn[T], targetFn vpack.PackFn[K]) *IndexInfo[K, T, uint16] {
	return IndexExt(dbInfo, name, termFn, vpack.FUInt16, targetFn)
}

func IndexExt[K, T, P comparable](dbInfo *Info, name string, termFn vpack.PackFn[T], priorityFn vpack.PackFn[P], targetFn vpack.PackFn[K]) *IndexInfo[K, T, P] {
	generic.Append(&dbInfo.IndexList, name)
	return &IndexInfo[K, T, P]{
		Name:     name,
		TargetPackFn: targetFn,
		TermPackFn:   termFn,
		PriorityPackFn: priorityFn,
	}
}


func termKeyPrefix[K, T, P comparable](info *IndexInfo[K, T, P], term *T) []byte {
	buf := vpack.NewWriter()
	buf.WriteBytes(IndexTermPrefix)
	info.TermPackFn(term, buf)
	return buf.Data
}

func targetKeyPrefix[K, T, P comparable](info *IndexInfo[K, T, P], target *K) []byte {
	buf := vpack.NewWriter()
	buf.WriteBytes(IndexTargetPrefix)
	info.TargetPackFn(target, buf)
	return buf.Data
}

func termTargetKey[K, T, P comparable](info *IndexInfo[K, T, P], target *K, term *T, priority *P) []byte {
	buf := vpack.NewWriter()
	buf.WriteBytes(IndexTermPrefix)
	info.TermPackFn(term, buf)
	info.PriorityPackFn(priority, buf)
	info.TargetPackFn(target, buf)
	return buf.Data
}

func termCountKey[K, T, P comparable](info *IndexInfo[K, T, P], term *T) []byte {
	buf := vpack.NewWriter()
	buf.WriteBytes(IndexCountPrefix)
	info.TermPackFn(term, buf)
	return buf.Data
}

func readTargetTerm[K, T, P comparable](info *IndexInfo[K, T, P], data []byte) (target K, term T) {
	buf := vpack.NewReader(data)
	buf.Pos++ // skip the IndexRevsrefix byte
	info.TargetPackFn(&target, buf)
	info.TermPackFn(&term, buf)
	return
}

func targetTermKey[K, T, P comparable](info *IndexInfo[K, T, P], target *K, term *T) []byte {
	buf := vpack.NewWriter()
	buf.WriteBytes(IndexTargetPrefix)
	info.TargetPackFn(target, buf)
	info.TermPackFn(term, buf)
	return buf.Data
}

var PackCountFn = vpack.Int

func incTermCount[K, T, P comparable](tx *Tx, info *IndexInfo[K, T, P], term *T, increment int) {
	key := termCountKey(info, term)
	bkt := TxRawBucket(tx, info.Name)
	v := bkt.Get(key)
	var count int
	vpack.FromBytesInto(v, &count, PackCountFn)
	count += increment
	RawMustPut(bkt, key, vpack.ToBytes(&count, PackCountFn))
}

func ReadTermCount[K, T, P comparable](tx *Tx, info *IndexInfo[K, T, P], term *T, count *int) bool {
	key := termCountKey(info, term)
	bkt := TxRawBucket(tx, info.Name)
	v := bkt.Get(key)
	return vpack.FromBytesInto(v, count, PackCountFn)
}

func addTargetTermPair[K, T, P comparable](tx *Tx, info *IndexInfo[K, T, P], target *K, term *T, priority *P) {
	val := vpack.ToBytes(priority, info.PriorityPackFn)
	bkt := TxRawBucket(tx, info.Name)
	bkt.Put(termTargetKey(info, target, term, priority), nil)
	bkt.Put(targetTermKey(info, target, term), val)
}

func delTargetTermPair[K, T, P comparable](tx *Tx, info *IndexInfo[K, T, P], target *K, term *T, priority *P) {
	targetTermKey := targetTermKey(info, target, term)
	bkt := TxRawBucket(tx, info.Name)
	bkt.Delete(termTargetKey(info, target, term, priority))
	bkt.Delete(targetTermKey)
}

func _PlainTerms[T, P comparable](terms []T) map[T]P {
	var zero P
	return UniformTerms(terms, zero)
}

func UniformTerms[T, P comparable](terms []T, priority P) (out map[T]P) {
	generic.InitMap(&out)
	for _, t := range terms {
		out[t] = priority
	}
	return
}

func SetTargetSingleTerm[K, T, P comparable](tx *Tx, info *IndexInfo[K, T, P], target K, term T) {
	SetTargetTerms(tx, info, target, _PlainTerms[T, P]([]T{term}))
}

func SetTargetSingleTermExt[K, T, P comparable](tx *Tx, info *IndexInfo[K, T, P], target K, priority P, term T) {
	SetTargetTerms(tx, info, target, UniformTerms([]T{term}, priority))
}


func DeleteTargetTerms[K, T, P comparable](tx *Tx, info *IndexInfo[K, T, P], target K) {
	SetTargetTerms(tx, info, target, nil)
}

// sets terms without priorities
func SetTargetTermsPlain[K, T, P comparable](tx *Tx, info *IndexInfo[K, T, P], target K, terms []T) {
	SetTargetTerms(tx, info, target, _PlainTerms[T, P](terms))
}

func SetTargetTermsUniform[K, T, P comparable](tx *Tx, info *IndexInfo[K, T, P], target K, terms []T, priority P) {
	SetTargetTerms(tx, info, target, UniformTerms(terms, priority))
}

// Updates target,term pairs so that only the terms provided here point to target.
// terms map the term to the priority
func SetTargetTerms[K, T, P comparable](tx *Tx, info *IndexInfo[K, T, P], target K, terms map[T]P) {
	var existing = make(map[T]P)

	// read out the list of existing index terms so we can get the list of actual bucket keys to add / remove
	IterateTarget(tx, info, target, func(term T, priority P) bool {
		existing[term] = priority
		return true
	})

	var add = make(map[T]P)
	var del = make(map[T]P)

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

func IterateTerm[K, T, P comparable](tx *Tx, info *IndexInfo[K, T, P], term T, visitFn func(target K, priority P) bool) []byte {
	return _IterateTermCore(tx, info, term, Window{}, visitFn)
}

func IterateTermOffset[K, T, P comparable](tx *Tx, info *IndexInfo[K, T, P], term T, offset int, visitFn func(target K, priority P) bool) []byte {
	options := Window{Offset: offset}
	return _IterateTermCore(tx, info, term, options, visitFn)
}

func ReadTermTargets[K, T, P comparable](tx *Tx, info *IndexInfo[K, T, P], term T, targets *[]K, window Window) []byte {
	return _IterateTermCore(tx, info, term, window, func(target K, priority P) bool {
		generic.Append(targets, target)
		return true
	})
}

func ReadTermTargetSingle[K, T, P comparable](tx *Tx, info *IndexInfo[K, T, P], term T, target *K) bool {
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
func _IterateTermCore[K, T, P comparable](tx *Tx, info *IndexInfo[K, T, P], term T, window Window, visitFn func(target K, priority P) bool) []byte {
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
func IterateTarget[K, T, P comparable](tx *Tx, info *IndexInfo[K, T, P], target K, visitFn func(term T, priority P) bool) {
	keyPrefix := targetKeyPrefix(info, &target)
	bkt := TxRawBucket(tx, info.Name)

	RawIterateKeyPrefixData(bkt.Cursor(), keyPrefix, func(key []byte, v []byte) bool {
		// we can safely assume the key starts with IndexTermPrefix because otherwise the RawIterateKeyPrefixValues func will not call us
		target, term := readTargetTerm(info, key)
		var priority P
		vpack.FromBytesInto(v, &priority, info.PriorityPackFn)
		_ = target
		return visitFn(term, priority)
	})
}

func readTermTargetPriority[K, T, P comparable](info *IndexInfo[K, T, P], data []byte) (term T, target K, priority P) {
	buf := vpack.NewReader(data)
	buf.Pos++ // skip the IndexTermPrefix byte
	info.TermPackFn(&term, buf)
	info.PriorityPackFn(&priority, buf)
	info.TargetPackFn(&target, buf)
	return
}

func IterateAllTerms[K, T, P comparable](tx *Tx, info *IndexInfo[K, T, P], visitFn func(term T, target K, priority P) bool) {
	var keyPrefix = []byte{IndexTermPrefix}
	bkt := TxRawBucket(tx, info.Name)

	RawIterateKeyPrefixData(bkt.Cursor(), keyPrefix, func(key []byte, v []byte) bool {
		term, target, priority := readTermTargetPriority(info, key)
		return visitFn(term, target, priority)
	})
}
