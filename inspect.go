package vbolt

import (
	"fmt"
	"log"
	"reflect"
	"strings"

	"go.hasen.dev/generic"
	"go.hasen.dev/vpack"
)

type GenericItem struct {
	Key   any
	Value any
}

type Inspection struct {
	// request
	BucketInfoPtr any // must be a *BucketInfo[K, V]
	Limit         int

	// both request/response
	NextKey any

	// response
	Items           []GenericItem
	TotalItemsCount int
}

// GenericRead takes a generic bucketInfo (must be *BucketInfo, i.e. a pointer)
// and reads a list of keys and values without really knowing the underlying type.
// Introspection is needed to properly display the type. Formatting the data as JSON is a good start.
func GenericRead(tx *Tx, inspection *Inspection) {
	bucketInfoValue := reflect.ValueOf(inspection.BucketInfoPtr).Elem()
	keyFn := bucketInfoValue.FieldByName("KeyPackFn")
	serFn := bucketInfoValue.FieldByName("ValuePackFn")
	name := bucketInfoValue.FieldByName("Name").String()

	seek := reflectPack(keyFn, inspection.NextKey)

	bkt := TxRawBucket(tx, name)
	crsr := bkt.Cursor()
	k, v := crsr.Seek(seek)

	generic.Reset(&inspection.Items)

	for k != nil && len(inspection.Items) < inspection.Limit {
		var item GenericItem
		item.Key = reflectUnpack(keyFn, k)
		item.Value = reflectUnpack(serFn, v)
		generic.Append(&inspection.Items, item)
		k, v = crsr.Next()
	}
	inspection.NextKey = reflectUnpack(keyFn, k)

	inspection.TotalItemsCount = bkt.Stats().KeyN
	return
}

func reflectPack(serFn reflect.Value, data any) []byte {
	if data == nil {
		return nil
	}
	buf := vpack.NewWriter()
	serFn.Call([]reflect.Value{
		reflect.ValueOf(data),
		reflect.ValueOf(buf),
	})
	return buf.Data
}

func reflectUnpack(serFn reflect.Value, data []byte) any {
	objectType := serFn.Type().In(0).Elem()
	obj := reflect.New(objectType)
	if data != nil {
		serFn.Call([]reflect.Value{
			obj,
			reflect.ValueOf(vpack.NewReader(data)),
		})
	}
	return obj.Interface()
}

func DEBUGInspect[K, V any](tx *Tx, bucket *BucketInfo[K, V]) {
	var inspection Inspection
	inspection.BucketInfoPtr = bucket
	inspection.Limit = 1000
	GenericRead(tx, &inspection)
	var b strings.Builder
	b.WriteString(bucket.Name + ":\n")
	for _, item := range inspection.Items {
		fmt.Fprint(&b, generic.JSONify(item.Key, ""))
		fmt.Fprint(&b, "=>  ")
		fmt.Fprint(&b, generic.JSONify(item.Value, ""))
	}
	fmt.Fprint(&b, "Total Count:", inspection.TotalItemsCount)
	log.Println(b.String())
}
