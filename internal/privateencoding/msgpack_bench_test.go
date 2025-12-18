package privateencoding_test

// the purpose of this file is to benchmark msgpack for easy comparison with this package

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

type benchStruct struct {
	Int    int
	String string
	Nested struct {
		Int    int
		String string
	}
}

func benchmarkMsgpackEncodeDecode[T any](b *testing.B, value T) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	dec := msgpack.NewDecoder(&buf)

	for b.Loop() {
		err := enc.Encode(value)
		assert.NoError(b, err)

		var out T
		err = dec.Decode(&out)
		assert.NoError(b, err)
	}
}

func benchmarkMsgpackEncodeOnly[T any](b *testing.B, value T) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	b.ResetTimer()
	for b.Loop() {
		buf.Reset()
		err := enc.Encode(value)
		assert.NoError(b, err)
	}
}

func benchmarkMsgpackDecodeOnly[T any](b *testing.B, value T) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	err := enc.Encode(value)
	assert.NoError(b, err)

	b.ResetTimer()
	for b.Loop() {
		var out T
		dec := msgpack.NewDecoder(bytes.NewReader(buf.Bytes()))
		err := dec.Decode(&out)
		assert.NoError(b, err)
	}
}

func BenchmarkMsgpackEncodeDecodeBool(b *testing.B) {
	benchmarkMsgpackEncodeDecode(b, true)
}

func BenchmarkMsgpackEncodeDecodeInt(b *testing.B) {
	benchmarkMsgpackEncodeDecode[int](b, 1)
}

func BenchmarkMsgpackEncodeDecodeString(b *testing.B) {
	benchmarkMsgpackEncodeDecode(b, "test")
}

func BenchmarkMsgpackEncodeDecodeBytes(b *testing.B) {
	benchmarkMsgpackEncodeDecode(b, []byte("test"))
}

func BenchmarkMsgpackEncodeDecodeSlice(b *testing.B) {
	benchmarkMsgpackEncodeDecode(b, []int{1, 2, 3})
}

func BenchmarkMsgpackEncodeDecodeMap(b *testing.B) {
	benchmarkMsgpackEncodeDecode(b, map[string]int{"test": 1})
}

func BenchmarkMsgpackEncodeDecodeStruct(b *testing.B) {
	value := benchStruct{
		Int:    1,
		String: "test",
	}
	value.Nested.Int = 1
	value.Nested.String = "test"

	benchmarkMsgpackEncodeDecode(b, value)
}

func BenchmarkMsgpackEncodeDecodeTime(b *testing.B) {
	benchmarkMsgpackEncodeDecode(b, time.Now())
}

// Focused encode/decode benchmarks for complex types

func BenchmarkMsgpackEncodeOnlyStruct(b *testing.B) {
	value := benchStruct{
		Int:    1,
		String: "test",
	}
	value.Nested.Int = 1
	value.Nested.String = "test"

	benchmarkMsgpackEncodeOnly(b, value)
}

func BenchmarkMsgpackDecodeOnlyStruct(b *testing.B) {
	value := benchStruct{
		Int:    1,
		String: "test",
	}
	value.Nested.Int = 1
	value.Nested.String = "test"

	benchmarkMsgpackDecodeOnly(b, value)
}

func BenchmarkMsgpackEncodeOnlyTime(b *testing.B) {
	benchmarkMsgpackEncodeOnly(b, time.Now())
}

func BenchmarkMsgpackDecodeOnlyTime(b *testing.B) {
	benchmarkMsgpackDecodeOnly(b, time.Now())
}
