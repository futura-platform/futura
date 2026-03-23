package privateencoding_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/futura-platform/futura/ftype/seal"
	"github.com/futura-platform/futura/moment"
	"github.com/futura-platform/futura/privateencoding"
	"github.com/samber/mo"
	"github.com/futura-platform/futura/privateencoding/internal/otherpackage"
	"github.com/stretchr/testify/assert"
	"k8s.io/utils/diff"
)

type TestRun struct {
	Name  string
	Value any
}

type unexportedComparable struct {
	unexportedField int
}

type myInterface interface {
	SomeMethod() string
}
type myImplementation struct {
	SomeField        string
	somePrivateField int
}

func (s *myImplementation) SomeMethod() string {
	return s.SomeField
}

func TestCodec(t *testing.T) {
	// nil values
	var nilPtr *int
	t.Run("nil_pointer", codecTest(nilPtr))
	var nilInterface any
	t.Run("nil_interface", codecTest(nilInterface))
	var nilMap map[string]any
	t.Run("nil_map", codecTest(nilMap))
	var nilSlice []any
	t.Run("nil_slice", codecTest(nilSlice))
	var nilBytes []byte
	t.Run("nil_bytes", codecTest(nilBytes))
	// string like
	t.Run("[]byte", codecTest([]byte("Hello, 世界")))
	t.Run("string", codecTest("Hello, 世界"))
	t.Run("[]rune", codecTest([]rune("Hello, 世界")))
	// complex types
	a := 1
	t.Run("Pointer", codecTest(otherpackage.NewCodecTestStruct(&a)))
	t.Run("Slice", codecTest(otherpackage.NewCodecTestStruct([]int{1, 2, 3})))
	t.Run("Map", codecTest(otherpackage.NewCodecTestStruct(map[string]int{"a": 1, "b": 2})))
	t.Run("Struct", codecTest(otherpackage.NewCodecTestStruct(struct{ A int }{A: 1})))

	// All numeric primitives
	t.Run("int8", codecTest(int8(-128)))
	t.Run("int16", codecTest(int16(-32768)))
	t.Run("int32", codecTest(int32(-2147483648)))
	t.Run("int64", codecTest(int64(-9223372036854775808)))
	t.Run("uint8", codecTest(uint8(255)))
	t.Run("uint16", codecTest(uint16(65535)))
	t.Run("uint32", codecTest(uint32(4294967295)))
	t.Run("uint64", codecTest(uint64(18446744073709551615)))
	t.Run("float32", codecTest(float32(3.14159)))
	t.Run("float64", codecTest(float64(3.141592653589793)))
	t.Run("bool_true", codecTest(true))
	t.Run("bool_false", codecTest(false))
	t.Run("uint", codecTest(uint(123456789)))
	t.Run("int", codecTest(int(-123456789)))

	// Zero values
	t.Run("zero_int", codecTest(0))
	t.Run("zero_float", codecTest(0.0))
	t.Run("empty_string", codecTest(""))
	t.Run("empty_slice", codecTest([]int{}))
	t.Run("empty_map", codecTest(map[string]int{}))
	t.Run("empty_bytes", codecTest([]byte{}))

	// Nested structures
	t.Run("nested_slice", codecTest([][]int{{1, 2}, {3, 4}}))
	t.Run("nested_map", codecTest(map[string]map[string]int{"outer": {"inner": 42}}))
	t.Run("slice_of_structs", codecTest([]struct{ X int }{{X: 1}, {X: 2}}))
	t.Run("map_of_structs", codecTest(map[string]struct{ Y int }{"a": {Y: 10}}))

	// Deeply nested
	type Deep struct {
		Level1 struct {
			Level2 struct {
				Value int
			}
		}
	}
	t.Run("deeply_nested", codecTest(Deep{Level1: struct {
		Level2 struct{ Value int }
	}{Level2: struct{ Value int }{Value: 999}}}))

	// Slices of primitives (fast path in intDataSize)
	t.Run("[]int8", codecTest([]int8{-1, 0, 1}))
	t.Run("[]int16", codecTest([]int16{-1, 0, 1}))
	t.Run("[]int32", codecTest([]int32{-1, 0, 1}))
	t.Run("[]int64", codecTest([]int64{-1, 0, 1}))
	t.Run("[]uint16", codecTest([]uint16{0, 1, 2}))
	t.Run("[]uint32", codecTest([]uint32{0, 1, 2}))
	t.Run("[]uint64", codecTest([]uint64{0, 1, 2}))
	t.Run("[]float32", codecTest([]float32{1.1, 2.2, 3.3}))
	t.Run("[]float64", codecTest([]float64{1.1, 2.2, 3.3}))
	t.Run("[]bool", codecTest([]bool{true, false, true}))

	// Named types (type aliases) - all primitive types
	type MyBool bool
	t.Run("named_bool", codecTest(MyBool(true)))

	type MyString string
	t.Run("named_string", codecTest(MyString("hello")))

	type MyInt int
	t.Run("named_int", codecTest(MyInt(42)))
	type MyInt8 int8
	t.Run("named_int8", codecTest(MyInt8(-128)))
	type MyInt16 int16
	t.Run("named_int16", codecTest(MyInt16(-32768)))
	type MyInt32 int32
	t.Run("named_int32", codecTest(MyInt32(-2147483648)))
	type MyInt64 int64
	t.Run("named_int64", codecTest(MyInt64(-9223372036854775808)))

	type MyUint uint
	t.Run("named_uint", codecTest(MyUint(123456789)))
	type MyUint8 uint8
	t.Run("named_uint8", codecTest(MyUint8(255)))
	type MyUint16 uint16
	t.Run("named_uint16", codecTest(MyUint16(65535)))
	type MyUint32 uint32
	t.Run("named_uint32", codecTest(MyUint32(4294967295)))
	type MyUint64 uint64
	t.Run("named_uint64", codecTest(MyUint64(18446744073709551615)))
	type MyUintptr uintptr
	t.Run("named_uintptr", codecTest(MyUintptr(0xDEADBEEF)))

	type MyByte byte
	t.Run("named_byte", codecTest(MyByte(0xFF)))
	type MyRune rune
	t.Run("named_rune", codecTest(MyRune('世')))

	type MyFloat32 float32
	t.Run("named_float32", codecTest(MyFloat32(3.14159)))
	type MyFloat64 float64
	t.Run("named_float64", codecTest(MyFloat64(3.141592653589793)))

	type MyComplex64 complex64
	t.Run("named_complex64", codecTest(MyComplex64(1+2i)))
	type MyComplex128 complex128
	t.Run("named_complex128", codecTest(MyComplex128(3+4i)))

	// Struct with multiple field types
	type MixedStruct struct {
		Int    int
		Float  float64
		String string
		Bytes  []byte
		Nested struct{ X int }
	}
	t.Run("mixed_struct", codecTest(MixedStruct{
		Int:    42,
		Float:  3.14,
		String: "test",
		Bytes:  []byte{1, 2, 3},
		Nested: struct{ X int }{X: 100},
	}))

	// Map with various key types
	t.Run("map_int_keys", codecTest(map[int]string{1: "one", 2: "two"}))
	t.Run("map_unexported_comparable_keys", codecTest(map[unexportedComparable]int{{1}: 1}))

	// Unexported fields (via otherpackage)
	t.Run("unexported_fields", codecTest(otherpackage.NewMyStruct(10, 20)))

	// Pointer chains
	b := 42
	pb := &b
	t.Run("double_pointer", codecTest(otherpackage.NewCodecTestStruct(&pb)))

	// Large data (performance)
	largeSlice := make([]int, 10000)
	for i := range largeSlice {
		largeSlice[i] = i
	}
	t.Run("large_slice", codecTest(largeSlice))

	largeMap := make(map[int]int, 1000)
	for i := range 1000 {
		largeMap[i] = i * 2
	}
	t.Run("large_map", codecTest(largeMap))

	// Unicode edge cases
	t.Run("unicode_emoji", codecTest("👋🌍🎉"))
	t.Run("unicode_mixed", codecTest("Hello 世界 🌍 مرحبا"))

	// interfaces with custom implementations
	t.Run("custom_interface", func(t *testing.T) {
		var customEncoderDecoder myInterface = &myImplementation{SomeField: "test", somePrivateField: 42}
		privateencoding.Register[*myImplementation]()
		applyCodecThenCompare(t, customEncoderDecoder, nil)
	})

	// misc standard library types
	t.Run("time.Time", func(t *testing.T) {
		time_ := time.Now()
		applyCodecThenCompare(t, time_, func(a, b time.Time) bool { return a.Equal(b) })
	})
	t.Run("cookiejar.Jar", func(t *testing.T) {
		cookieJar, err := cookiejar.New(nil)
		assert.NoError(t, err)
		u := &url.URL{
			Scheme: "https",
			Host:   "example.com",
		}
		cookieJar.SetCookies(u, []*http.Cookie{{Name: "test", Value: "test"}})
		applyCodecThenCompare(t, cookieJar, func(a, b *cookiejar.Jar) bool {
			return reflect.DeepEqual(a.Cookies(u), b.Cookies(u))
		})
	})

	// sealed types
	t.Run("sealed_types", func(t *testing.T) {
		a := seal.Seal(1)
		applyCodecThenCompare(t, a, nil)

		b := seal.Seal("b")
		applyCodecThenCompare(t, b, nil)

		c := seal.Seal(struct{ A int }{A: 1})
		applyCodecThenCompare(t, c, nil)

		d := seal.Seal(map[string]int{"a": 1})
		applyCodecThenCompare(t, d, nil)
	})

	// any casted
	t.Run("any_casted", func(t *testing.T) {
		a := any(1)
		applyCodecThenCompare(t, a, nil)
	})

	// binary marshalable
	t.Run("binary_marshalable_ptr_unmarshaller", func(t *testing.T) {
		b := binaryMarshalablePtrUnmarshaller{}
		applyCodecThenCompare(t, b, func(a, b binaryMarshalablePtrUnmarshaller) bool {
			return b.didUnmarshal
		})
	})

	t.Run("binary_marshalable_direct_unmarshaller", func(t *testing.T) {
		b := binaryMarshalableDirectUnmarshaller{}
		_, err := applyCodec(b)
		assert.ErrorIs(t, err, errDirectUnmarshaller)
	})

	t.Run("moment_identity", func(t *testing.T) {
		id := moment.NewIdentity(t.Context(), moment.Callpath{{File: "test.go", Line: 1}})
		applyCodecThenCompare(t, id, nil)
	})
}

func TestCodecURLPointer(t *testing.T) {
	value := &url.URL{
		Scheme:   "https",
		Host:     "example.com",
		Path:     "/search",
		RawQuery: "q=futura",
	}

	assert.NotPanics(t, func() {
		decoded, err := applyCodec(value)
		assert.NoError(t, err)
		if assert.NotNil(t, decoded) {
			assert.Equal(t, value.String(), decoded.String())
		}
	})
}

func TestCodecNilURLPointer(t *testing.T) {
	var value *url.URL

	assert.NotPanics(t, func() {
		decoded, err := applyCodec(value)
		assert.NoError(t, err)
		assert.Nil(t, decoded)
	})
}

func TestCodecURLPointerInMomentOutput(t *testing.T) {
	privateencoding.Register[*url.URL]()
	value := *moment.NewMoment(moment.NewFn(func(ctx context.Context, args int) (int, error) {
		return args, nil
	}), 1)
	value.SetValidOutput(&url.URL{
		Scheme:   "https",
		Host:     "example.com",
		Path:     "/search",
		RawQuery: "q=" + strings.Repeat("f", 300),
	})

	assert.NotPanics(t, func() {
		decoded, err := applyCodec(value)
		assert.NoError(t, err)
		if assert.IsType(t, &url.URL{}, decoded.Output().MustGet()) {
			assert.Equal(
				t,
				value.Output().MustGet().(*url.URL).String(),
				decoded.Output().MustGet().(*url.URL).String(),
			)
		}
	})
}

// TestCodecMoOptionSkipsBinaryMarshaler guards against using mo.Option's
// encoding.BinaryMarshaler implementation, which serializes via gob and breaks
// privateencoding (see isMoOptionType in encoder.go).
func TestCodecMoOptionSkipsBinaryMarshaler(t *testing.T) {
	t.Run("some_int", func(t *testing.T) {
		applyCodecThenCompare(t, mo.Some(42), nil)
	})
	t.Run("none_int", func(t *testing.T) {
		applyCodecThenCompare(t, mo.None[int](), nil)
	})
	t.Run("some_any", func(t *testing.T) {
		applyCodecThenCompare(t, mo.Some[any](7), nil)
	})
	t.Run("nested_in_struct", func(t *testing.T) {
		type holder struct {
			O mo.Option[string]
		}
		applyCodecThenCompare(t, holder{O: mo.Some("encoded")}, nil)
	})
}

func codecTest[T any](value T) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("direct", func(t *testing.T) {
			applyCodecThenCompare(t, value, nil)
		})
		t.Run("in_struct", func(t *testing.T) {
			testStruct := *otherpackage.NewCodecTestStruct(value)
			applyCodecThenCompare(t, testStruct, nil)
		})
		t.Run("type_alias", func(t *testing.T) {
			type MyType = T
			applyCodecThenCompare(t, MyType(value), nil)
		})
	}
}

type binaryMarshalablePtrUnmarshaller struct {
	didUnmarshal bool
}

func (b binaryMarshalablePtrUnmarshaller) MarshalBinary() ([]byte, error) {
	return []byte("marshalled"), nil
}

func (b *binaryMarshalablePtrUnmarshaller) UnmarshalBinary(data []byte) error {
	if string(data) != "marshalled" {
		return fmt.Errorf("invalid data")
	}
	b.didUnmarshal = true
	return nil
}

type binaryMarshalableDirectUnmarshaller struct{}

func (b binaryMarshalableDirectUnmarshaller) MarshalBinary() ([]byte, error) {
	return []byte("marshalled"), nil
}

var errDirectUnmarshaller = fmt.Errorf("did call direct unmarshaller")

func (b *binaryMarshalableDirectUnmarshaller) UnmarshalBinary(data []byte) error {
	if string(data) != "marshalled" {
		return fmt.Errorf("invalid data")
	}
	return errDirectUnmarshaller
}

func applyCodecThenCompare[T any](t *testing.T, value T, compareOverride func(T, T) bool) {
	compare := compareOverride
	if compare == nil {
		compare = func(a, b T) bool { return reflect.DeepEqual(a, b) }
	}
	result, err := applyCodec(value)
	assert.NoError(t, err)
	assert.True(t, compare(value, result), diff.ObjectGoPrintSideBySide(value, result))
}

func applyCodec[T any](value T) (T, error) {
	buf := bytes.NewBuffer(nil)
	codec := privateencoding.NewEncoder[T](buf)
	err := codec.Encode(value)
	if err != nil {
		return value, err
	}

	decoder := privateencoding.NewDecoder[T](buf)
	decoded, err := decoder.Decode()
	if err != nil {
		return value, err
	}
	return decoded, nil
}
