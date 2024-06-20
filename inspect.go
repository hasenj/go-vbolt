package vbolt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"go.hasen.dev/generic"
	"go.hasen.dev/vpack"
)

func JSONify(obj any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "")
	b, _ := json.Marshal(obj)
	return string(b)
}

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
	keyFn := bucketInfoValue.FieldByName("KeyFn")
	serFn := bucketInfoValue.FieldByName("SerializeFn")
	name := bucketInfoValue.FieldByName("Name").String()

	seek := reflectSerialize(keyFn, inspection.NextKey)

	bkt := TxRawBucket(tx, name)
	crsr := bkt.Cursor()
	k, v := crsr.Seek(seek)

	generic.Reset(&inspection.Items)

	for k != nil && len(inspection.Items) < inspection.Limit {
		var item GenericItem
		item.Key = reflectDeserialize(keyFn, k)
		item.Value = reflectDeserialize(serFn, v)
		generic.Append(&inspection.Items, item)
		k, v = crsr.Next()
	}
	inspection.NextKey = reflectDeserialize(keyFn, k)

	inspection.TotalItemsCount = bkt.Stats().KeyN
	return
}

func reflectSerialize(serFn reflect.Value, data any) []byte {
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

func reflectDeserialize(serFn reflect.Value, data []byte) any {
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
	for _, item := range inspection.Items {
		fmt.Println(JSONify(item.Key))
		fmt.Println(JSONify(item.Value))
	}
	fmt.Println("Total Count:", inspection.TotalItemsCount)
}
