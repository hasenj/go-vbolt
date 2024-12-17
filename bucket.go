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
		KeyPackFn:   keyFn,
		ValuePackFn: serFn,
	}
	dbInfo.Infos[name] = result
	return result
}

func HasKey[K, T any](tx *Tx, bucketInfo *BucketInfo[K, T], id K) bool {
	bkt := TxRawBucket(tx, bucketInfo.Name)
	return RawHasKey(bkt, vpack.ToBytes(&id, bucketInfo.KeyPackFn))
}

func Read[K comparable, T any](tx *Tx, bucketInfo *BucketInfo[K, T], id K, item *T) bool {
	bkt := TxRawBucket(tx, bucketInfo.Name)
	return _Read(bkt, bucketInfo, id, item)
}

func _Read[K comparable, T any](bkt *BBucket, bucketInfo *BucketInfo[K, T], id K, item *T) bool {
	if bkt == nil {
		return false
	}
	var zero K
	if id == zero {
		return false
	}
	key := vpack.ToBytes(&id, bucketInfo.KeyPackFn)
	data := bkt.Get(key)
	if data == nil {
		return false
	}
	return vpack.FromBytesInto(data, item, bucketInfo.ValuePackFn)
}

// ReadSlice reads objects given by ids, appending them to the given slice.
// returns the number of objects that were successfully read
func ReadSlice[K comparable, T any](tx *Tx, bucketInfo *BucketInfo[K, T], ids []K, list *[]T) int {
	bkt := TxRawBucket(tx, bucketInfo.Name)
	if bkt == nil {
		return 0
	}
	count := 0
	for _, id := range ids {
		var item T
		if _Read(bkt, bucketInfo, id, &item) {
			generic.Append(list, item)
			count++
		}
	}
	return count
}

// ReadSliceToMap reads objects given by id into the given map.
// returns the number of objects that were successfully read
func ReadSliceToMap[K comparable, T any](tx *Tx, bucketInfo *BucketInfo[K, T], ids []K, itemsMap map[K]T) int {
	bkt := TxRawBucket(tx, bucketInfo.Name)
	if bkt == nil {
		return 0
	}
	count := 0
	for _, id := range ids {
		var item T
		if _Read(bkt, bucketInfo, id, &item) {
			itemsMap[id] = item
			count++
		}
	}
	return count
}

// like read slice but for reading one item and appending it to a list
func ReadAppend[K comparable, T any](tx *Tx, bucketInfo *BucketInfo[K, T], id K, list *[]T) bool {
	var item T
	if Read(tx, bucketInfo, id, &item) {
		generic.Append(list, item)
		return true
	} else {
		return false
	}
}

func ReadToMap[K comparable, T any](tx *Tx, bucketInfo *BucketInfo[K, T], id K, itemsMap map[K]T) bool {
	var item T
	if Read(tx, bucketInfo, id, &item) {
		itemsMap[id] = item
		return true
	} else {
		return false
	}
}

// Writes an item to a key. Note: does not write anything if id is the zero value
func Write[K comparable, T any](tx *Tx, bucketInfo *BucketInfo[K, T], id K, item *T) {
	var zero K
	if id == zero {
		return
	}
	bkt := TxRawBucket(tx, bucketInfo.Name)
	key := vpack.ToBytes(&id, bucketInfo.KeyPackFn)
	data := vpack.ToBytes(item, bucketInfo.ValuePackFn)
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

func _IterateAllCore[K, T any](bkt *BBucket, bucketInfo *BucketInfo[K, T], direction IterationDirection, visitFn func(key K, item T) bool) {
	var iterParams _RawIterationParams
	iterParams.Direction = direction

	_RawIterateCore(bkt, iterParams, func(key []byte, value []byte) bool {
		var itemKey K
		var item T
		vpack.FromBytesInto(key, &itemKey, bucketInfo.KeyPackFn)
		vpack.FromBytesInto(value, &item, bucketInfo.ValuePackFn)
		return visitFn(itemKey, item)
	})
}

func IterateAll[K, T any](tx *Tx, bucketInfo *BucketInfo[K, T], visitFn func(key K, item T) bool) {
	bkt := TxRawBucket(tx, bucketInfo.Name)
	_IterateAllCore(bkt, bucketInfo, IterateRegular, visitFn)
}

func IterateAllReverse[K, T any](tx *Tx, bucketInfo *BucketInfo[K, T], visitFn func(key K, item T) bool) {
	bkt := TxRawBucket(tx, bucketInfo.Name)
	_IterateAllCore(bkt, bucketInfo, IterateReverse, visitFn)
}

func IterateInBatches[K, T any](tx *Tx, bucketInfo *BucketInfo[K, T], batchSize int, visitFn func(items []T) bool) {
	list := make([]T, 0, batchSize)
	var key K
	var done bool // iterator is done
	for !done {
		generic.ShrinkTo(&list, 0)
		key, done = ScanList(tx, bucketInfo, key, batchSize, &list)
		if !visitFn(list) {
			break
		}
	}
}

func ScanList[K, T any](tx *Tx, bucketInfo *BucketInfo[K, T], startKey K, count int, items *[]T) (nextKey K, done bool) {
	bkt := TxRawBucket(tx, bucketInfo.Name)

	var iterParams _RawIterationParams
	iterParams.Prefix = []byte{}
	iterParams.Cursor = vpack.ToBytes(&startKey, bucketInfo.KeyPackFn)
	iterParams.Direction = IterateRegular
	iterParams.Limit = count

	nextKeyBytes := _RawIterateCore(bkt, iterParams, func(key []byte, value []byte) bool {
		var item T
		vpack.FromBytesInto(value, &item, bucketInfo.ValuePackFn)
		generic.Append(items, item)
		return true
	})
	done = nextKeyBytes == nil
	if !done {
		vpack.FromBytesInto(nextKeyBytes, &nextKey, bucketInfo.KeyPackFn)
	}
	return
}

// IterateBucketFrom lets you specify the starting key using the userspace key type
func IterateBucketFrom[K, T any](tx *Tx, bucketInfo *BucketInfo[K, T], startKey K, visitFn func(key K, value T) bool) []byte {
	bkt := TxRawBucket(tx, bucketInfo.Name)

	var iterParams _RawIterationParams
	iterParams.Prefix = vpack.ToBytes(&startKey, bucketInfo.KeyPackFn)

	return _RawIterateCore(bkt, iterParams, func(key []byte, value []byte) bool {
		var itemKey K
		var item T
		vpack.FromBytesInto(key, &itemKey, bucketInfo.KeyPackFn)
		vpack.FromBytesInto(value, &item, bucketInfo.ValuePackFn)
		return visitFn(itemKey, item)
	})
}
