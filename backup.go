package vbolt

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"go.hasen.dev/generic"
)

const BUCKET_HEADER byte = 0x01
const ITEM_HEADER byte = 0x02

func ChannelError(target *error, err error) {
	if err != nil {
		// TODO: accumulate?
		*target = err
	}
}

type _BackupBuilder struct {
	Output *bufio.Writer
	Error  error
}

func _BackupIoWrite(builder *_BackupBuilder, p []byte) {
	if builder.Error != nil {
		return
	}
	_, err := builder.Output.Write(p)
	ChannelError(&builder.Error, err)
}

func _BackupWriteByte(builder *_BackupBuilder, b byte) {
	if builder.Error != nil {
		return
	}
	err := builder.Output.WriteByte(b)
	ChannelError(&builder.Error, err)
	// _BackupIoWrite(builder, []byte{b})
}

func _BackupWriteBuffer(builder *_BackupBuilder, buf []byte) {
	lenBytes := binary.AppendUvarint(nil, uint64(len(buf)))
	_BackupIoWrite(builder, lenBytes)
	_BackupIoWrite(builder, buf)
}

func _BackupWriteBucketHeader(builder *_BackupBuilder, bucketNameBytes []byte) {
	_BackupWriteByte(builder, BUCKET_HEADER)
	_BackupWriteBuffer(builder, bucketNameBytes)
}

func _BackupWriteItem(builder *_BackupBuilder, key []byte, value []byte) {
	_BackupWriteByte(builder, ITEM_HEADER)
	_BackupWriteBuffer(builder, key)
	_BackupWriteBuffer(builder, value)
}

type _BackupReader struct {
	Input *bytes.Reader
	Error error
}

func _BackupReadByte(reader *_BackupReader) byte {
	if reader.Error != nil {
		return 0
	}
	b, err := reader.Input.ReadByte()
	ChannelError(&reader.Error, err)
	return b
}

// allocates a new buffer every time, but this is required for writing to bolt
// DO NOT attempt to optimize this by minimizing allocations. It will not work
// with bolt transactions properly
func _BackupReadBuffer(reader *_BackupReader) []byte {
	if reader.Error != nil {
		return nil
	}
	sizeu64, err := binary.ReadUvarint(reader.Input)
	// fmt.Println("size:", sizeu64)
	ChannelError(&reader.Error, err)
	size := int(sizeu64)
	buffer := make([]byte, size)
	n, err := reader.Input.Read(buffer)
	ChannelError(&reader.Error, err)
	buffer = buffer[:n]
	return buffer
}

func BackupBuckets(db *DB, out *bufio.Writer, bucketNames ...string) error {
	tx := ViewTx(db)
	defer TxClose(tx)

	var backup _BackupBuilder
	backup.Output = out

	for _, bucketName := range bucketNames {
		if backup.Error != nil {
			break
		}
		bucketNameBytes := []byte(bucketName)
		bkt := tx.Bucket(bucketNameBytes)
		if bkt == nil { // skip invalid bucket names
			fmt.Println("Warning: invalid bucket name supplied to backup process:", bucketName)
			continue
		}
		_BackupWriteBucketHeader(&backup, bucketNameBytes)
		bkt.ForEach(func(key []byte, value []byte) error {
			_BackupWriteItem(&backup, key, value)
			return backup.Error
		})
	}

	return backup.Error
}

func RestoreBuckets(db *DB, in *bytes.Reader) error {
	var reader = new(_BackupReader)
	reader.Input = in
	var bucketName []byte
	var key []byte
	var value []byte

	tx := WriteTx(db)
	defer func() { // this is to prevent the defer from fixating on the current tx
		// and allow it to work with whatever tx is at the end of the function ..
		TxClose(tx)
	}()

	var bucket *BBucket
	var writesCount int
	const txThreshold = 1024 * 4

	var totalCount int

	for {
		b := _BackupReadByte(reader)
		switch b {
		case BUCKET_HEADER:
			// fmt.Println("Restoring bucket", generic.UnsafeString(bucketName))
			bucketName = _BackupReadBuffer(reader)
			bucket = TxRawBucket(tx, generic.UnsafeString(bucketName))
		case ITEM_HEADER:
			key = _BackupReadBuffer(reader)
			value = _BackupReadBuffer(reader)
			RawMustPut(bucket, key, value)
			totalCount++
			writesCount++
			fmt.Printf("%d     \r", totalCount)
			if writesCount > txThreshold {
				TxCommit(tx)
				tx = WriteTx(db)
				writesCount = 0
				bucket = TxRawBucket(tx, generic.UnsafeString(bucketName))
			}
		default:
			fmt.Println("Total restored items:", totalCount)
			TxCommit(tx)
			if reader.Error == io.EOF {
				return nil
			} else {
				fmt.Println("Final value is:", b)
				fmt.Println("Error:", reader.Error)
				return reader.Error
			}
		}
	}
}

func DumpBucketJSON[K, V any](db *DB, out *bufio.Writer, label string, bucket *BucketInfo[K, V]) {
	tx := ViewTx(db)
	defer TxClose(tx)
	enc := json.NewEncoder(out)
	IterateAll(tx, bucket, func(key K, value V) bool {
		out.WriteString(label)
		enc.Encode(value)
		return true
	})
}
