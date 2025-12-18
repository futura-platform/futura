package privateencoding_test

import (
	"bytes"
	"encoding/gob"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/futura-platform/futura/internal/privateencoding"
)

func benchmarkEncodeDecode[T any](b *testing.B, value T) {
	var buf bytes.Buffer
	enc := privateencoding.NewEncoder[T](&buf)
	dec := privateencoding.NewDecoder[T](&buf)
	for b.Loop() {
		err := enc.Encode(value)
		assert.NoError(b, err)

		_, err = dec.Decode()
		assert.NoError(b, err)
	}
}

func BenchmarkEncodeDecodeBool(b *testing.B) {
	benchmarkEncodeDecode(b, true)
}

func BenchmarkEncodeDecodeInt(b *testing.B) {
	benchmarkEncodeDecode[int](b, 1)
}

func BenchmarkEncodeDecodeInt8(b *testing.B) {
	benchmarkEncodeDecode[int8](b, 1)
}

func BenchmarkEncodeDecodeInt16(b *testing.B) {
	benchmarkEncodeDecode[int16](b, 1)
}

func BenchmarkEncodeDecodeInt32(b *testing.B) {
	benchmarkEncodeDecode[int32](b, 1)
}

func BenchmarkEncodeDecodeInt64(b *testing.B) {
	benchmarkEncodeDecode[int64](b, 1)
}

func BenchmarkEncodeDecodeUint(b *testing.B) {
	benchmarkEncodeDecode[uint](b, 1)
}

func BenchmarkEncodeDecodeUint8(b *testing.B) {
	benchmarkEncodeDecode[uint8](b, 1)
}

func BenchmarkEncodeDecodeUint16(b *testing.B) {
	benchmarkEncodeDecode[uint16](b, 1)
}

func BenchmarkEncodeDecodeUint32(b *testing.B) {
	benchmarkEncodeDecode[uint32](b, 1)
}

func BenchmarkEncodeDecodeUint64(b *testing.B) {
	benchmarkEncodeDecode[uint64](b, 1)
}

func BenchmarkEncodeDecodeFloat32(b *testing.B) {
	benchmarkEncodeDecode[float32](b, 1.0)
}

func BenchmarkEncodeDecodeFloat64(b *testing.B) {
	benchmarkEncodeDecode[float64](b, 1.0)
}

func BenchmarkEncodeDecodeComplex64(b *testing.B) {
	benchmarkEncodeDecode[complex64](b, 1.0)
}

func BenchmarkEncodeDecodeComplex128(b *testing.B) {
	benchmarkEncodeDecode[complex128](b, 1.0)
}

func BenchmarkEncodeDecodeString(b *testing.B) {
	benchmarkEncodeDecode(b, "test")
}

func BenchmarkEncodeDecodeBytes(b *testing.B) {
	benchmarkEncodeDecode(b, []byte("test"))
}

func BenchmarkEncodeDecodeSlice(b *testing.B) {
	benchmarkEncodeDecode(b, []int{1, 2, 3})
}

func BenchmarkEncodeDecodeMap(b *testing.B) {
	benchmarkEncodeDecode(b, map[string]int{"test": 1})
}

func BenchmarkEncodeDecodeStruct(b *testing.B) {
	benchmarkEncodeDecode(b, struct {
		Int    int
		String string
		Nested struct {
			Int    int
			String string
		}
	}{1, "test", struct {
		Int    int
		String string
	}{1, "test"}})
}

func BenchmarkEncodeDecodeInterface(b *testing.B) {
	benchmarkEncodeDecode(b, any(1))
}

func BenchmarkCustomEncoderDecoder(b *testing.B) {
	gob.Register(&myImplementation{})
	var customEncoderDecoder myInterface = &myImplementation{SomeField: "test"}
	benchmarkEncodeDecode(b, customEncoderDecoder)
}

func BenchmarkEncodeDecodeTime(b *testing.B) {
	benchmarkEncodeDecode(b, time.Now())
}
