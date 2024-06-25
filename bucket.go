package vbolt

import (
	"go.hasen.dev/generic"
	"go.hasen.dev/vpack"
)

type BucketInfo[K, T any] struct {
	Name        string
	KeyPackFn   vpack.PackFn[K]
	ValuePackFn vpack.PackFn[T]
}

func Bucket[K, T any](dbInfo *Info, name string, keyFn vpack.PackFn[K], serFn vpack.PackFn[T]) *BucketInfo[K, T] {
	generic.Append(&dbInfo.BucketList, name)
	generic.EnsureMapNotNil(&dbInfo.Infos)
	result := &BucketInfo[K, T]{
		Name:        name,
		KeyPackFn:       keyFn,
		ValuePackFn: serFn,
	}
	dbInfo.Infos[name] = result
	return result
}

func HasKey[K, T any](tx *Tx, info *BucketInfo[K, T], id K) bool {
	bkt := TxRawBucket(tx, info.Name)
	return RawHasKey(bkt, vpack.ToBytes(&id, info.KeyPackFn))
}

func Read[K comparable, T any](tx *Tx, info *BucketInfo[K, T], id K, item *T) bool {
	bkt := TxRawBucket(tx, info.Name)
	return _Read(bkt, info, id, item)
}

func _Read[K comparable, T any](bkt *BBucket, info *BucketInfo[K, T], id K, item *T) bool {
	if bkt == nil {
		return false
	}
	var zero K
	if id == zero {
		return false
	}
	key := vpack.ToBytes(&id, info.KeyPackFn)
	data := bkt.Get(key)
	if data == nil {
		return false
	}
	return vpack.FromBytesInto(data, item, info.ValuePackFn)
}

// ReadSlice reads objects given by ids, appending them to the given slice.
// returns the number of objects that were successfully read
func ReadSlice[K comparable, T any](tx *Tx, info *BucketInfo[K, T], ids []K, list *[]T) int {
	bkt := TxRawBucket(tx, info.Name)
	if bkt == nil {
		return 0
	}
	count := 0
	for _, id := range ids {
		var item T
		if _Read(bkt, info, id, &item) {
			generic.Append(list, item)
			count++
		}
	}
	return count
}

// ReadSliceToMap reads objects given by id into the given map.
// returns the number of objects that were successfully read
func ReadSliceToMap[K comparable, T any](tx *Tx, info *BucketInfo[K, T], ids []K, itemsMap map[K]T) int {
	bkt := TxRawBucket(tx, info.Name)
	if bkt == nil {
		return 0
	}
	count := 0
	for _, id := range ids {
		var item T
		if _Read(bkt, info, id, &item) {
			itemsMap[id] = item
			count++
		}
	}
	return count
}

// like read slice but for reading one item and appending it to a list
func ReadAppend[K comparable, T any](tx *Tx, info *BucketInfo[K, T], id K, list *[]T) bool {
	var item T
	if Read(tx, info, id, &item) {
		generic.Append(list, item)
		return true
	} else {
		return false
	}
}

func ReadToMap[K comparable, T any](tx *Tx, info *BucketInfo[K, T], id K, itemsMap map[K]T) bool {
	var item T
	if Read(tx, info, id, &item) {
		itemsMap[id] = item
		return true
	} else {
		return false
	}
}

// Writes an item to a key. Note: does not write anything if id is the zero value
func Write[K comparable, T any](tx *Tx, info *BucketInfo[K, T], id K, item *T) {
	var zero K
	if id == zero {
		return
	}
	bkt := TxRawBucket(tx, info.Name)
	key := vpack.ToBytes(&id, info.KeyPackFn)
	data := vpack.ToBytes(item, info.ValuePackFn)
	RawMustPut(bkt, key, data)
}

func Delete[K, T any](tx *Tx, info *BucketInfo[K, T], id K) {
	bkt := TxRawBucket(tx, info.Name)
	key := vpack.ToBytes(&id, info.KeyPackFn)
	bkt.Delete(key)
}

func NextIntId[K, T any](tx *Tx, info *BucketInfo[K, T]) int {
	bkt := TxRawBucket(tx, info.Name)
	return int(RawNextSequence(bkt))
}

type _IterationDirection uint8

const _IterateRegular = _IterationDirection(0)
const _IterateReverse = _IterationDirection(1)

func _CursorStartPos(c *Cursor, direction _IterationDirection) (k []byte, v []byte) {
	if direction == _IterateRegular {
		return c.First()
	}
	if direction == _IterateReverse {
		return c.Last()
	}
	return
}

func _CursorStep(c *Cursor, direction _IterationDirection) (k []byte, v []byte) {
	if direction == _IterateRegular {
		return c.Next()
	}
	if direction == _IterateReverse {
		return c.Prev()
	}
	return
}

func _IterateCore[K, T any](bkt *BBucket, info *BucketInfo[K, T], direction _IterationDirection, visitFn func(key K, item T) bool) {
	crsr := bkt.Cursor()
	key, value := _CursorStartPos(crsr, direction)
	for key != nil {
		itemKey := vpack.FromBytes(key, info.KeyPackFn)
		item := vpack.FromBytes(value, info.ValuePackFn)

		if itemKey == nil || item == nil {
			continue
		}

		if !visitFn(*itemKey, *item) {
			break
		}
		key, value = _CursorStep(crsr, direction)
	}
}

func IterateAll[K, T any](tx *Tx, info *BucketInfo[K, T], visitFn func(key K, item T) bool) {
	bkt := TxRawBucket(tx, info.Name)
	_IterateCore(bkt, info, _IterateRegular, visitFn)
}

func IterateAllReverse[K, T any](tx *Tx, info *BucketInfo[K, T], visitFn func(key K, item T) bool) {
	bkt := TxRawBucket(tx, info.Name)
	_IterateCore(bkt, info, _IterateReverse, visitFn)
}

func IterateInBatches[K, T any](tx *Tx, info *BucketInfo[K, T], batchSize int, visitFn func(items []T) bool) {
	list := make([]T, 0, batchSize)
	var key K
	var done bool // iterator is done
	for !done {
		generic.ShrinkTo(&list, 0)
		key, done = ScanList(tx, info, key, batchSize, &list)
		if !visitFn(list) {
			break
		}
	}
}

func ScanList[K, T any](tx *Tx, info *BucketInfo[K, T], startKey K, count int, items *[]T) (nextKey K, done bool) {
	bkt := TxRawBucket(tx, info.Name)
	crsr := bkt.Cursor()
	key, value := crsr.Seek(vpack.ToBytes(&startKey, info.KeyPackFn))
	for i := 0; i < count; i++ {
		if key == nil { // end of bucket
			break
		}
		var item T
		if vpack.FromBytesInto(value, &item, info.ValuePackFn) {
			generic.Append(items, item)
		} else {
			continue
		}
		key, value = crsr.Next()
	}
	done = key == nil
	if !done {
		vpack.FromBytesInto(key, &nextKey, info.KeyPackFn)
	}
	return
}
