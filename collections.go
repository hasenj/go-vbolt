package vbolt

import (
	"bytes"

	"go.hasen.dev/generic"
	"go.hasen.dev/vpack"
)

// Collections are deprecated.
// Just use indexes.
// The original reason I created them was becuase the index did not support
// traversing backwards and using a cursor for pagination.

// TODO:  migration function that converts a collection to an index

/*
	Collections are similar to indexes, but with some differences

	- A collection has a key, and it stores items ordered by some order key
		- overlap is possible
	- Collections can be iterated forward or backward
	- We can find out all the collections an item is in

	The key difference from indexes is:

	- An Item wants to control its membership in collections one at a time.
		- Although a "set all collections" action can be implemented on top of
		  those building blocks
		- Infact, an Index can probably be implemented on top of a collection
		  by just implementing that
		  - And perhaps we /should/ do that, as it could simplify the code!


	TODO: test the collections api!
*/

const CKeyPrefix byte = 0x10
const CItemPrefix byte = 0x12
const CCountPrefix byte = 0x13

// collection bucket
type CollectionInfo[K, O, I any] struct {
	Name string

	KeyFn   vpack.PackFn[K]
	OrderFn vpack.PackFn[O]
	ItemFn  vpack.PackFn[I]
}

func Collection[K, O, I any](dbInfo *Info, name string, keyFn vpack.PackFn[K], orderFn vpack.PackFn[O], itemFn vpack.PackFn[I]) *CollectionInfo[K, O, I] {
	generic.Append(&dbInfo.CollectionList, name)
	generic.EnsureMapNotNil(&dbInfo.Infos)
	result := &CollectionInfo[K, O, I]{
		Name:    name,
		KeyFn:   keyFn,
		OrderFn: orderFn,
		ItemFn:  itemFn,
	}
	dbInfo.Infos[name] = result
	return result
}

// The prefix for iterating on collection by key
func _CKeyPrefix[K, O, I any](info *CollectionInfo[K, O, I], key K) []byte {
	buf := vpack.NewWriter()
	buf.WriteBytes(CKeyPrefix)
	info.KeyFn(&key, buf)
	return buf.Data
}

// The prefix for iterating on item (get all collections for item)
func _CItemPrefix[K, O, I any](info *CollectionInfo[K, O, I], item I) []byte {
	buf := vpack.NewWriter()
	buf.WriteBytes(CItemPrefix)
	info.ItemFn(&item, buf)
	return buf.Data
}

// The full key for inserting
func _CKeyFull[K, O, I any](info *CollectionInfo[K, O, I], key K, order O, item I) []byte {
	buf := vpack.NewWriter()
	buf.WriteBytes(CKeyPrefix)
	info.KeyFn(&key, buf)
	info.OrderFn(&order, buf)
	info.ItemFn(&item, buf)
	return buf.Data
}

func _CRevKeyValue[K, O, I any](info *CollectionInfo[K, O, I], key K, order O, item I) (bKey []byte, bValue []byte) {
	buf := vpack.NewWriter()
	buf.WriteBytes(CItemPrefix)
	info.ItemFn(&item, buf)
	info.KeyFn(&key, buf)
	bKey = buf.Data
	bValue = vpack.ToBytes(&order, info.OrderFn)
	return
}

func _CCountKey[K, O, I any](info *CollectionInfo[K, O, I], key K) []byte {
	buf := vpack.NewWriter()
	buf.WriteBytes(CCountPrefix)
	info.KeyFn(&key, buf)
	return buf.Data
}

func _ReadKeyOrderItem[K, O, I any](info *CollectionInfo[K, O, I], bKey []byte) (key K, order O, item I) {
	buf := vpack.NewReader(bKey)
	buf.Pos++ // skip the prefix byte
	info.KeyFn(&key, buf)
	info.OrderFn(&order, buf)
	info.ItemFn(&item, buf)
	return
}

// TODO: take a "start at" param
func _IterateCollectionCore[K, O, I any](tx *Tx, info *CollectionInfo[K, O, I], key K, direction IterationDirection, visit func(key K, order O, item I) bool) {
	prefix := _CKeyPrefix(info, key)

	window := _RawIterationParams{
		Prefix: prefix,
		Window: Window{
			Direction: direction,
		},
	}

	_RawIterateCore(TxRawBucket(tx, info.Name), window, func(bKey []byte, bValue []byte) bool {
		key, order, item := _ReadKeyOrderItem(info, bKey)
		return visit(key, order, item)
	})
}

func IterateCollection[K, O, I any](tx *Tx, info *CollectionInfo[K, O, I], key K, visit func(key K, order O, item I) bool) {
	_IterateCollectionCore(tx, info, key, IterateRegular, visit)
}

func IterateCollectionReverse[K, O, I any](tx *Tx, info *CollectionInfo[K, O, I], key K, visit func(key K, order O, item I) bool) {
	_IterateCollectionCore(tx, info, key, IterateReverse, visit)
}

func ReadCollection[K, O, I any](tx *Tx, info *CollectionInfo[K, O, I], key K, items *[]I, count int) {
	if count < 0 {
		return
	}

	var added int
	IterateCollection(tx, info, key, func(_k K, _o O, item I) bool {
		generic.Append(items, item)
		added += 1
		return added < count
	})
}

func ReadCollectionReverse[K, O, I any](tx *Tx, info *CollectionInfo[K, O, I], key K, items *[]I, count int) {
	if count < 0 {
		return
	}

	var added int
	IterateCollectionReverse(tx, info, key, func(_k K, _o O, item I) bool {
		generic.Append(items, item)
		added += 1
		return added < count
	})
}

func _IncCount[K, O, I any](tx *Tx, info *CollectionInfo[K, O, I], key K, inc int) {
	bkt := TxRawBucket(tx, info.Name)
	bKey := _CCountKey(info, key)
	bValue := bkt.Get(bKey)
	var count int
	fn := vpack.Int
	vpack.FromBytesInto(bKey, &count, fn)
	count += inc
	bValue = vpack.ToBytes(&count, fn)
	bkt.Put(bKey, bValue)
}

func CollectionAddEntry[K, O, I any](tx *Tx, info *CollectionInfo[K, O, I], key K, order O, item I) {
	bkt := TxRawBucket(tx, info.Name)

	var exists bool
	var eOrder O // existing order (if exists)

	// item key and value
	iKey, iValue := _CRevKeyValue(info, key, order, item)
	{
		// check if this already exists
		crsr := bkt.Cursor()
		// existing key and vlaue
		eKey, eValue := crsr.Seek(iKey)
		if bytes.Equal(iKey, eKey) {
			exists = true
			if bytes.Equal(iValue, eValue) {
				// already exists with the same order, nothing to do
				return
			}
			vpack.FromBytesInto(eValue, &eOrder, info.OrderFn)
		}
	}

	if exists {
		// delete the existing
		bkt.Delete(_CKeyFull(info, key, eOrder, item))
		bkt.Put(_CKeyFull(info, key, order, item), nil)
		bkt.Put(iKey, iValue)
	} else {
		bkt.Put(_CKeyFull(info, key, order, item), nil)
		bkt.Put(iKey, iValue)
		_IncCount(tx, info, key, 1)
	}
}

func CollectionRemoveEntry[K, O, I any](tx *Tx, info *CollectionInfo[K, O, I], key K, item I) {
	bkt := TxRawBucket(tx, info.Name)

	var order O // starts out as the zero order

	// item key
	iKey, _ := _CRevKeyValue(info, key, order, item)

	// check if this already exists
	crsr := bkt.Cursor()
	// existing key and vlaue
	eKey, eValue := crsr.Seek(iKey)
	if !bytes.Equal(iKey, eKey) {
		// entry does not exist, nothing to do
		return
	}
	vpack.FromBytesInto(eValue, &order, info.OrderFn)

	// delete the entry, the reverse entry, and decrease the count
	bkt.Delete(_CKeyFull(info, key, order, item))
	bkt.Delete(iKey)
	_IncCount(tx, info, key, -1)
}
