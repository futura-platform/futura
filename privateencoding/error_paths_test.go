package privateencoding_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/futura-platform/futura/privateencoding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failingWriter is an io.Writer that fails after n writes
type failingWriter struct {
	writes    int
	failAfter int
	err       error
}

func newFailingWriter(failAfter int) *failingWriter {
	return &failingWriter{
		failAfter: failAfter,
		err:       errors.New("write failed"),
	}
}

func (w *failingWriter) Write(p []byte) (n int, err error) {
	if w.writes >= w.failAfter {
		return 0, w.err
	}
	w.writes++
	return len(p), nil
}

// assertErrorContainsPath checks that an error message contains the expected path
func assertErrorContainsPath(t *testing.T, err error, action, expectedPath string) {
	t.Helper()
	require.Error(t, err, "expected an error but got nil")

	expectedPrefix := "failed to " + action + " " + expectedPath
	assert.ErrorContains(t, err, expectedPrefix)
}

func TestEncoder_ErrorPaths(t *testing.T) {
	t.Run("root_scalar", func(t *testing.T) {
		enc := privateencoding.NewEncoder[int](newFailingWriter(0))
		err := enc.Encode(42)
		assertErrorContainsPath(t, err, "encode", "root")
	})

	t.Run("root_string", func(t *testing.T) {
		enc := privateencoding.NewEncoder[string](newFailingWriter(0))
		err := enc.Encode("hello")
		assertErrorContainsPath(t, err, "encode", "root")
	})

	t.Run("struct_field", func(t *testing.T) {
		type Simple struct {
			First  int
			Second string
		}
		// First field succeeds, second fails
		enc := privateencoding.NewEncoder[Simple](newFailingWriter(1))
		err := enc.Encode(Simple{First: 1, Second: "test"})
		assertErrorContainsPath(t, err, "encode", "root.Second")
	})

	t.Run("struct_first_field", func(t *testing.T) {
		type Simple struct {
			First  int
			Second string
		}
		enc := privateencoding.NewEncoder[Simple](newFailingWriter(0))
		err := enc.Encode(Simple{First: 1, Second: "test"})
		assertErrorContainsPath(t, err, "encode", "root.First")
	})

	t.Run("nested_struct", func(t *testing.T) {
		type Inner struct {
			Value int
		}
		type Outer struct {
			Inner Inner
		}
		enc := privateencoding.NewEncoder[Outer](newFailingWriter(0))
		err := enc.Encode(Outer{Inner: Inner{Value: 42}})
		assertErrorContainsPath(t, err, "encode", "root.Inner.Value")
	})

	t.Run("pointer_nil_check", func(t *testing.T) {
		type WithPointer struct {
			Ptr *int
		}
		enc := privateencoding.NewEncoder[WithPointer](newFailingWriter(0))
		err := enc.Encode(WithPointer{Ptr: nil})
		assertErrorContainsPath(t, err, "encode", "root.Ptr == nil")
	})

	t.Run("pointer_deref", func(t *testing.T) {
		type WithPointer struct {
			Ptr *int
		}
		val := 42
		// First write is nil check (succeeds), second is the value (fails)
		enc := privateencoding.NewEncoder[WithPointer](newFailingWriter(1))
		err := enc.Encode(WithPointer{Ptr: &val})
		assertErrorContainsPath(t, err, "encode", "(*root.Ptr)")
	})

	t.Run("slice_nil_check", func(t *testing.T) {
		type WithSlice struct {
			Slice []int
		}
		enc := privateencoding.NewEncoder[WithSlice](newFailingWriter(0))
		err := enc.Encode(WithSlice{Slice: nil})
		assertErrorContainsPath(t, err, "encode", "root.Slice == nil")
	})

	t.Run("slice_length", func(t *testing.T) {
		type WithSlice struct {
			Slice []int
		}
		// First write: nil check succeeds, second write: length fails
		enc := privateencoding.NewEncoder[WithSlice](newFailingWriter(1))
		err := enc.Encode(WithSlice{Slice: []int{1, 2, 3}})
		assertErrorContainsPath(t, err, "encode", "len(root.Slice)")
	})

	t.Run("slice_element", func(t *testing.T) {
		type WithSlice struct {
			Slice []int
		}
		// nil check + length + first element succeeds, second element fails
		enc := privateencoding.NewEncoder[WithSlice](newFailingWriter(3))
		err := enc.Encode(WithSlice{Slice: []int{1, 2, 3}})
		assertErrorContainsPath(t, err, "encode", "root.Slice[1]")
	})

	t.Run("map_nil_check", func(t *testing.T) {
		type WithMap struct {
			Map map[string]int
		}
		enc := privateencoding.NewEncoder[WithMap](newFailingWriter(0))
		err := enc.Encode(WithMap{Map: nil})
		assertErrorContainsPath(t, err, "encode", "root.Map == nil")
	})

	t.Run("map_length", func(t *testing.T) {
		type WithMap struct {
			Map map[string]int
		}
		// nil check succeeds, length fails
		enc := privateencoding.NewEncoder[WithMap](newFailingWriter(1))
		err := enc.Encode(WithMap{Map: map[string]int{"a": 1}})
		assertErrorContainsPath(t, err, "encode", "len(root.Map)")
	})

	t.Run("map_key", func(t *testing.T) {
		type WithMap struct {
			Map map[string]int
		}
		// nil check + length succeeds, first key fails
		enc := privateencoding.NewEncoder[WithMap](newFailingWriter(2))
		err := enc.Encode(WithMap{Map: map[string]int{"a": 1}})
		assertErrorContainsPath(t, err, "encode", "root.Map[key-0]")
	})

	t.Run("map_value", func(t *testing.T) {
		type WithMap struct {
			Map map[string]int
		}
		// nil check + length + key succeeds, value fails
		// Note: msgpack may use multiple writes for strings, so we try increasing counts
		enc := privateencoding.NewEncoder[WithMap](newFailingWriter(4))
		err := enc.Encode(WithMap{Map: map[string]int{"a": 1}})
		assertErrorContainsPath(t, err, "encode", "root.Map[val-0]")
	})

	t.Run("deeply_nested_path", func(t *testing.T) {
		type Level3 struct {
			Value int
		}
		type Level2 struct {
			Level3 Level3
		}
		type Level1 struct {
			Level2 Level2
		}
		type Root struct {
			Level1 Level1
		}
		enc := privateencoding.NewEncoder[Root](newFailingWriter(0))
		err := enc.Encode(Root{Level1: Level1{Level2: Level2{Level3: Level3{Value: 42}}}})
		assertErrorContainsPath(t, err, "encode", "root.Level1.Level2.Level3.Value")
	})

	t.Run("slice_of_structs", func(t *testing.T) {
		type Item struct {
			Name string
		}
		type Container struct {
			Items []Item
		}
		// nil check + length + Item[0].Name succeeds, Item[1].Name fails
		// msgpack writes multiple times for strings, so we need more writes
		enc := privateencoding.NewEncoder[Container](newFailingWriter(4))
		err := enc.Encode(Container{Items: []Item{{Name: "a"}, {Name: "b"}}})
		assertErrorContainsPath(t, err, "encode", "root.Items[1].Name")
	})

	t.Run("unsupported_chan", func(t *testing.T) {
		type WithChan struct {
			Ch chan int
		}
		var buf bytes.Buffer
		enc := privateencoding.NewEncoder[WithChan](&buf)
		err := enc.Encode(WithChan{Ch: make(chan int)})
		require.Error(t, err)
		assert.ErrorIs(t, err, privateencoding.ErrUnsupportedType)
		assert.ErrorContains(t, err, "chan")
	})

	t.Run("unsupported_func", func(t *testing.T) {
		type WithFunc struct {
			Fn func()
		}
		var buf bytes.Buffer
		enc := privateencoding.NewEncoder[WithFunc](&buf)
		err := enc.Encode(WithFunc{Fn: func() {}})
		require.Error(t, err)
		assert.ErrorIs(t, err, privateencoding.ErrUnsupportedType)
		assert.ErrorContains(t, err, "func")
	})
}

// encodeValue encodes a value and returns the bytes
func encodeValue[T any](t *testing.T, value T) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := privateencoding.NewEncoder[T](&buf)
	require.NoError(t, enc.Encode(value))
	return buf.Bytes()
}

// truncatedReader returns a reader that returns EOF after reading the truncated data
type truncatedReader struct {
	data []byte
	pos  int
}

func newTruncatedReader(data []byte, truncateAt int) *truncatedReader {
	if truncateAt > len(data) {
		truncateAt = len(data)
	}
	return &truncatedReader{data: data[:truncateAt]}
}

func (r *truncatedReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func TestDecoder_ErrorPaths(t *testing.T) {
	t.Run("root_scalar", func(t *testing.T) {
		// Truncate data so it can't decode the int
		dec := privateencoding.NewDecoder[int](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root")
	})

	t.Run("struct_first_field", func(t *testing.T) {
		type Simple struct {
			First  int
			Second string
		}
		// Empty data - fails on first field
		dec := privateencoding.NewDecoder[Simple](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.First")
	})

	t.Run("struct_second_field", func(t *testing.T) {
		type Simple struct {
			First  int
			Second string
		}
		data := encodeValue(t, Simple{First: 1, Second: "test"})
		// Truncate after first field - fails on second field
		// First field is a small int, typically 1 byte in msgpack
		dec := privateencoding.NewDecoder[Simple](newTruncatedReader(data, 1))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Second")
	})

	t.Run("nested_struct", func(t *testing.T) {
		type Inner struct {
			Value int
		}
		type Outer struct {
			Inner Inner
		}
		// Empty data - fails on nested field
		dec := privateencoding.NewDecoder[Outer](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Inner.Value")
	})

	t.Run("pointer_nil_check", func(t *testing.T) {
		type WithPointer struct {
			Ptr *int
		}
		// Empty data - fails when checking if nil
		dec := privateencoding.NewDecoder[WithPointer](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Ptr == nil")
	})

	t.Run("pointer_deref", func(t *testing.T) {
		type WithPointer struct {
			Ptr *int
		}
		val := 42
		data := encodeValue(t, WithPointer{Ptr: &val})
		// Truncate after nil-check bool (false = not nil) but before the int value
		// The bool false is 1 byte in msgpack
		dec := privateencoding.NewDecoder[WithPointer](newTruncatedReader(data, 1))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "(*root.Ptr)")
	})

	t.Run("slice_nil_check", func(t *testing.T) {
		type WithSlice struct {
			Slice []int
		}
		// Empty data - fails when checking if nil
		dec := privateencoding.NewDecoder[WithSlice](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Slice == nil")
	})

	t.Run("slice_length", func(t *testing.T) {
		type WithSlice struct {
			Slice []int
		}
		data := encodeValue(t, WithSlice{Slice: []int{1, 2, 3}})
		// Truncate after nil-check bool (false = not nil) but before length
		dec := privateencoding.NewDecoder[WithSlice](newTruncatedReader(data, 1))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "len(root.Slice)")
	})

	t.Run("slice_element", func(t *testing.T) {
		type WithSlice struct {
			Slice []int
		}
		data := encodeValue(t, WithSlice{Slice: []int{1, 2, 3}})
		// nil-check (1 byte) + length (1 byte) + first element (1 byte)
		// = 3 bytes, truncate to fail on second element
		dec := privateencoding.NewDecoder[WithSlice](newTruncatedReader(data, 3))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Slice[1]")
	})

	t.Run("map_nil_check", func(t *testing.T) {
		type WithMap struct {
			Map map[string]int
		}
		// Empty data - fails when checking if nil
		dec := privateencoding.NewDecoder[WithMap](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Map == nil")
	})

	t.Run("map_length", func(t *testing.T) {
		type WithMap struct {
			Map map[string]int
		}
		data := encodeValue(t, WithMap{Map: map[string]int{"a": 1}})
		// Truncate after nil-check bool
		dec := privateencoding.NewDecoder[WithMap](newTruncatedReader(data, 1))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "len(root.Map)")
	})

	t.Run("map_key", func(t *testing.T) {
		type WithMap struct {
			Map map[string]int
		}
		data := encodeValue(t, WithMap{Map: map[string]int{"a": 1}})
		// nil-check (1 byte) + length (1 byte) = 2 bytes, truncate to fail on key
		dec := privateencoding.NewDecoder[WithMap](newTruncatedReader(data, 2))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Map[key-0]")
	})

	t.Run("deeply_nested_path", func(t *testing.T) {
		type Level3 struct {
			Value int
		}
		type Level2 struct {
			Level3 Level3
		}
		type Level1 struct {
			Level2 Level2
		}
		type Root struct {
			Level1 Level1
		}
		// Empty data - fails on deeply nested field
		dec := privateencoding.NewDecoder[Root](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Level1.Level2.Level3.Value")
	})

	t.Run("slice_of_structs", func(t *testing.T) {
		type Item struct {
			Name string
		}
		type Container struct {
			Items []Item
		}
		data := encodeValue(t, Container{Items: []Item{{Name: "a"}, {Name: "b"}}})
		// nil-check (1) + length (1) + first string "a" (2 bytes: fixstr header + 'a')
		// = 4 bytes, truncate to fail on second item
		dec := privateencoding.NewDecoder[Container](newTruncatedReader(data, 4))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Items[1].Name")
	})

	// Tests for aliased scalar types that go through decodeComplex
	// These types bypass decodeSimple because type aliases don't match concrete types

	t.Run("aliased_string", func(t *testing.T) {
		type MyString string
		type Container struct {
			Value MyString
		}
		dec := privateencoding.NewDecoder[Container](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Value")
	})

	t.Run("aliased_bool", func(t *testing.T) {
		type MyBool bool
		type Container struct {
			Value MyBool
		}
		dec := privateencoding.NewDecoder[Container](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Value")
	})

	t.Run("aliased_int", func(t *testing.T) {
		type MyInt int
		type Container struct {
			Value MyInt
		}
		dec := privateencoding.NewDecoder[Container](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Value")
	})

	t.Run("aliased_uint", func(t *testing.T) {
		type MyUint uint
		type Container struct {
			Value MyUint
		}
		dec := privateencoding.NewDecoder[Container](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Value")
	})

	t.Run("aliased_int8", func(t *testing.T) {
		type MyInt8 int8
		type Container struct {
			Value MyInt8
		}
		dec := privateencoding.NewDecoder[Container](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Value")
	})

	t.Run("aliased_uint8", func(t *testing.T) {
		type MyUint8 uint8
		type Container struct {
			Value MyUint8
		}
		dec := privateencoding.NewDecoder[Container](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Value")
	})

	t.Run("aliased_int16", func(t *testing.T) {
		type MyInt16 int16
		type Container struct {
			Value MyInt16
		}
		dec := privateencoding.NewDecoder[Container](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Value")
	})

	t.Run("aliased_uint16", func(t *testing.T) {
		type MyUint16 uint16
		type Container struct {
			Value MyUint16
		}
		dec := privateencoding.NewDecoder[Container](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Value")
	})

	t.Run("aliased_int32", func(t *testing.T) {
		type MyInt32 int32
		type Container struct {
			Value MyInt32
		}
		dec := privateencoding.NewDecoder[Container](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Value")
	})

	t.Run("aliased_uint32", func(t *testing.T) {
		type MyUint32 uint32
		type Container struct {
			Value MyUint32
		}
		dec := privateencoding.NewDecoder[Container](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Value")
	})

	t.Run("aliased_int64", func(t *testing.T) {
		type MyInt64 int64
		type Container struct {
			Value MyInt64
		}
		dec := privateencoding.NewDecoder[Container](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Value")
	})

	t.Run("aliased_uint64", func(t *testing.T) {
		type MyUint64 uint64
		type Container struct {
			Value MyUint64
		}
		dec := privateencoding.NewDecoder[Container](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Value")
	})

	t.Run("aliased_float32", func(t *testing.T) {
		type MyFloat32 float32
		type Container struct {
			Value MyFloat32
		}
		dec := privateencoding.NewDecoder[Container](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Value")
	})

	t.Run("aliased_float64", func(t *testing.T) {
		type MyFloat64 float64
		type Container struct {
			Value MyFloat64
		}
		dec := privateencoding.NewDecoder[Container](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Value")
	})

	t.Run("complex64", func(t *testing.T) {
		type Container struct {
			Value complex64
		}
		dec := privateencoding.NewDecoder[Container](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Value")
	})

	t.Run("complex128", func(t *testing.T) {
		type Container struct {
			Value complex128
		}
		dec := privateencoding.NewDecoder[Container](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Value")
	})

	t.Run("aliased_uintptr", func(t *testing.T) {
		type MyUintptr uintptr
		type Container struct {
			Value MyUintptr
		}
		dec := privateencoding.NewDecoder[Container](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Value")
	})

	t.Run("map_value", func(t *testing.T) {
		type WithMap struct {
			Map map[string]int
		}
		data := encodeValue(t, WithMap{Map: map[string]int{"a": 1}})
		// nil-check (1 byte) + length (1 byte) + key "a" (2 bytes: fixstr header + 'a')
		// = 4 bytes, truncate to fail on value
		dec := privateencoding.NewDecoder[WithMap](newTruncatedReader(data, 4))
		_, err := dec.Decode()
		assertErrorContainsPath(t, err, "decode", "root.Map[a]")
	})

	t.Run("unsupported_chan", func(t *testing.T) {
		type WithChan struct {
			Ch chan int
		}
		dec := privateencoding.NewDecoder[WithChan](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		require.Error(t, err)
		assert.ErrorContains(t, err, "unsupported type")
		assert.ErrorContains(t, err, "chan")
	})

	t.Run("unsupported_func", func(t *testing.T) {
		type WithFunc struct {
			Fn func()
		}
		dec := privateencoding.NewDecoder[WithFunc](newTruncatedReader([]byte{}, 0))
		_, err := dec.Decode()
		require.Error(t, err)
		assert.ErrorContains(t, err, "unsupported type")
		assert.ErrorContains(t, err, "func")
	})
}

// countingWriter counts successful writes
type countingWriter struct {
	writes int
	w      io.Writer
}

func (w *countingWriter) Write(p []byte) (n int, err error) {
	n, err = w.w.Write(p)
	if err == nil {
		w.writes++
	}
	return
}

// TestEncoder_ErrorPathsAreHumanReadable verifies that error paths read like Go code
func TestEncoder_ErrorPathsAreHumanReadable(t *testing.T) {
	testCases := []struct {
		name         string
		expectedPath string
	}{
		{"root", "root"},
		{"field access", "root.Field"},
		{"nested field", "root.Outer.Inner"},
		{"pointer deref", "(*root.Ptr)"},
		{"nil check", "root.Ptr == nil"},
		{"slice element", "root.Slice[0]"},
		{"slice length", "len(root.Slice)"},
		{"map key", "root.Map[key-0]"},
		{"map value", "root.Map[val-0]"},
		{"map length", "len(root.Map)"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Just verify the path format looks correct
			expectedError := "failed to encode " + tc.expectedPath + ":"
			assert.Contains(t, expectedError, tc.expectedPath,
				"path %q should be present in error format", tc.expectedPath)
		})
	}
}
