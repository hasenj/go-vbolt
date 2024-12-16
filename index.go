package vbolt

import (
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
	Name           string
	TargetPackFn   vpack.PackFn[K]
	TermPackFn     vpack.PackFn[T]
	PriorityPackFn vpack.PackFn[P]
}

func Index[K, T comparable](dbInfo *Info, name string, termFn vpack.PackFn[T], targetFn vpack.PackFn[K]) *IndexInfo[K, T, uint16] {
	return IndexExt(dbInfo, name, termFn, vpack.FUInt16, targetFn)
}

func IndexExt[K, T, P comparable](dbInfo *Info, name string, termFn vpack.PackFn[T], priorityFn vpack.PackFn[P], targetFn vpack.PackFn[K]) *IndexInfo[K, T, P] {
	generic.Append(&dbInfo.IndexList, name)
	return &IndexInfo[K, T, P]{
		Name:           name,
		TargetPackFn:   targetFn,
		TermPackFn:     termFn,
		PriorityPackFn: priorityFn,
	}
}

func _TermKeyPrefix[K, T, P comparable](indexInfo *IndexInfo[K, T, P], term *T) []byte {
	buf := vpack.NewWriter()
	buf.WriteBytes(IndexTermPrefix)
	indexInfo.TermPackFn(term, buf)
	return buf.Data
}

func _TargetKeyPrefix[K, T, P comparable](indexInfo *IndexInfo[K, T, P], target *K) []byte {
	buf := vpack.NewWriter()
	buf.WriteBytes(IndexTargetPrefix)
	indexInfo.TargetPackFn(target, buf)
	return buf.Data
}

func _TermTargetKey[K, T, P comparable](indexInfo *IndexInfo[K, T, P], target *K, term *T, priority *P) []byte {
	buf := vpack.NewWriter()
	buf.WriteBytes(IndexTermPrefix)
	indexInfo.TermPackFn(term, buf)
	indexInfo.PriorityPackFn(priority, buf)
	indexInfo.TargetPackFn(target, buf)
	return buf.Data
}

func _TermCountKey[K, T, P comparable](indexInfo *IndexInfo[K, T, P], term *T) []byte {
	buf := vpack.NewWriter()
	buf.WriteBytes(IndexCountPrefix)
	indexInfo.TermPackFn(term, buf)
	return buf.Data
}

func _ReadTargetTerm[K, T, P comparable](indexInfo *IndexInfo[K, T, P], data []byte) (target K, term T) {
	buf := vpack.NewReader(data)
	buf.Pos++ // skip the IndexRevsrefix byte
	indexInfo.TargetPackFn(&target, buf)
	indexInfo.TermPackFn(&term, buf)
	return
}

func _TargetTermKey[K, T, P comparable](indexInfo *IndexInfo[K, T, P], target *K, term *T) []byte {
	buf := vpack.NewWriter()
	buf.WriteBytes(IndexTargetPrefix)
	indexInfo.TargetPackFn(target, buf)
	indexInfo.TermPackFn(term, buf)
	return buf.Data
}

var PackCountFn = vpack.Int

func _IncTermCount[K, T, P comparable](tx *Tx, indexInfo *IndexInfo[K, T, P], term *T, increment int) {
	key := _TermCountKey(indexInfo, term)
	bkt := TxRawBucket(tx, indexInfo.Name)
	v := bkt.Get(key)
	var count int
	vpack.FromBytesInto(v, &count, PackCountFn)
	count += increment
	RawMustPut(bkt, key, vpack.ToBytes(&count, PackCountFn))
}

func ReadTermCount[K, T, P comparable](tx *Tx, indexInfo *IndexInfo[K, T, P], term *T, count *int) bool {
	key := _TermCountKey(indexInfo, term)
	bkt := TxRawBucket(tx, indexInfo.Name)
	v := bkt.Get(key)
	return vpack.FromBytesInto(v, count, PackCountFn)
}

func _AddTargetTermPair[K, T, P comparable](tx *Tx, indexInfo *IndexInfo[K, T, P], target *K, term *T, priority *P) {
	val := vpack.ToBytes(priority, indexInfo.PriorityPackFn)
	bkt := TxRawBucket(tx, indexInfo.Name)
	bkt.Put(_TermTargetKey(indexInfo, target, term, priority), nil)
	bkt.Put(_TargetTermKey(indexInfo, target, term), val)
}

func _DelTargetTermPair[K, T, P comparable](tx *Tx, indexInfo *IndexInfo[K, T, P], target *K, term *T, priority *P) {
	targetTermKey := _TargetTermKey(indexInfo, target, term)
	bkt := TxRawBucket(tx, indexInfo.Name)
	bkt.Delete(_TermTargetKey(indexInfo, target, term, priority))
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

func SetTargetSingleTerm[K, T, P comparable](tx *Tx, indexInfo *IndexInfo[K, T, P], target K, term T) {
	SetTargetTerms(tx, indexInfo, target, _PlainTerms[T, P]([]T{term}))
}

func SetTargetSingleTermExt[K, T, P comparable](tx *Tx, indexInfo *IndexInfo[K, T, P], target K, priority P, term T) {
	SetTargetTerms(tx, indexInfo, target, UniformTerms([]T{term}, priority))
}

func DeleteTargetTerms[K, T, P comparable](tx *Tx, indexInfo *IndexInfo[K, T, P], target K) {
	SetTargetTerms(tx, indexInfo, target, nil)
}

// sets terms without priorities
func SetTargetTermsPlain[K, T, P comparable](tx *Tx, indexInfo *IndexInfo[K, T, P], target K, terms []T) {
	SetTargetTerms(tx, indexInfo, target, _PlainTerms[T, P](terms))
}

func SetTargetTermsUniform[K, T, P comparable](tx *Tx, indexInfo *IndexInfo[K, T, P], target K, terms []T, priority P) {
	SetTargetTerms(tx, indexInfo, target, UniformTerms(terms, priority))
}

// Updates target,term pairs so that only the terms provided here point to target.
// terms map the term to the priority
func SetTargetTerms[K, T, P comparable](tx *Tx, indexInfo *IndexInfo[K, T, P], target K, terms map[T]P) {
	var existing = make(map[T]P)

	// read out the list of existing index terms so we can get the list of actual bucket keys to add / remove
	IterateTarget(tx, indexInfo, target, func(term T, priority P) bool {
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
		_DelTargetTermPair(tx, indexInfo, &target, &term, &priority)
		_IncTermCount(tx, indexInfo, &term, -1)
	}

	for term, priority := range add {
		_AddTargetTermPair(tx, indexInfo, &target, &term, &priority)
		_IncTermCount(tx, indexInfo, &term, 1)
	}
}

func IterateTerm[K, T, P comparable](tx *Tx, indexInfo *IndexInfo[K, T, P], term T, visitFn func(target K, priority P) bool) []byte {
	return _IterateTermCore(tx, indexInfo, term, Window{}, visitFn)
}

func ReadTermTargets[K, T, P comparable](tx *Tx, indexInfo *IndexInfo[K, T, P], term T, targets *[]K, window Window) []byte {
	return _IterateTermCore(tx, indexInfo, term, window, func(target K, priority P) bool {
		generic.Append(targets, target)
		return true
	})
}

func ReadTermTargetSingle[K, T, P comparable](tx *Tx, indexInfo *IndexInfo[K, T, P], term T, target *K) bool {
	var targets []K
	var opts Window
	opts.Limit = 1
	ReadTermTargets(tx, indexInfo, term, &targets, opts)
	if len(targets) > 0 {
		*target = targets[0]
		return true
	} else {
		return false
	}

}

type Window struct {
	Limit     int // 0 means unlimited
	Offset    int // if both offset and cursor are set, cursor is used
	Cursor    []byte
	Direction IterationDirection
}

// iterate over targets that are assigned to term
func _IterateTermCore[K, T, P comparable](tx *Tx, indexInfo *IndexInfo[K, T, P], term T, window Window, visitFn func(target K, priority P) bool) []byte {
	keyPrefix := _TermKeyPrefix(indexInfo, &term)

	bkt := TxRawBucket(tx, indexInfo.Name)

	var iterParams = _RawIterationParams{
		Prefix: keyPrefix,
		Window: window,
	}

	return _RawIterateCore(bkt, iterParams, func(key []byte, v []byte) bool {
		// we can safely assume the key starts with IndexTermPrefix because
		// _RawIterateCore would not have called us otherwise
		_, target, priority := _ReadTermTargetPriority(indexInfo, key)
		return visitFn(target, priority)
	})
}

// iterate over terms that are assigned to target
func IterateTarget[K, T, P comparable](tx *Tx, indexInfo *IndexInfo[K, T, P], target K, visitFn func(term T, priority P) bool) {
	keyPrefix := _TargetKeyPrefix(indexInfo, &target)
	bkt := TxRawBucket(tx, indexInfo.Name)
	window := _RawIterationParams{
		Prefix: keyPrefix,
		Window: Window{
			Direction: IterateRegular,
		},
	}
	_RawIterateCore(bkt, window, func(key []byte, v []byte) bool {
		// we can safely assume the key starts with IndexTermPrefix because otherwise the RawIterateKeyPrefixValues func will not call us
		target, term := _ReadTargetTerm(indexInfo, key)
		var priority P
		vpack.FromBytesInto(v, &priority, indexInfo.PriorityPackFn)
		_ = target
		return visitFn(term, priority)
	})
}

func _ReadTermTargetPriority[K, T, P comparable](indexInfo *IndexInfo[K, T, P], data []byte) (term T, target K, priority P) {
	buf := vpack.NewReader(data)
	buf.Pos++ // skip the IndexTermPrefix byte
	indexInfo.TermPackFn(&term, buf)
	indexInfo.PriorityPackFn(&priority, buf)
	indexInfo.TargetPackFn(&target, buf)
	return
}

func IterateAllTerms[K, T, P comparable](tx *Tx, indexInfo *IndexInfo[K, T, P], visitFn func(term T, target K, priority P) bool) {
	var keyPrefix = []byte{IndexTermPrefix}
	bkt := TxRawBucket(tx, indexInfo.Name)

	window := _RawIterationParams{
		Prefix: keyPrefix,
		Window: Window{
			Direction: IterateRegular,
		},
	}

	_RawIterateCore(bkt, window, func(key []byte, v []byte) bool {
		term, target, priority := _ReadTermTargetPriority(indexInfo, key)
		return visitFn(term, target, priority)
	})
}
