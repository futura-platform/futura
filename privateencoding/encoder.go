package privateencoding

import (
	"bytes"
	"encoding"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	privateencodinginternal "github.com/futura-platform/futura/privateencoding/internal"
)

// Encoder is used to serialize values of type T to a binary format,
// it serializes exported AND unexported fields of the type T.
type Encoder[T any] struct {
	w io.Writer
	// msgpackEncoder is used as a fast path for primitive leaf values
	// (ints, floats, bools, strings, []byte) at any depth.
	msgpackEncoder *msgpack.Encoder
}

func NewEncoder[T any](w io.Writer) *Encoder[T] {
	return &Encoder[T]{
		w:              w,
		msgpackEncoder: msgpack.NewEncoder(w),
	}
}

func (e *Encoder[T]) Encode(data T) error {
	// Always go through encodeValue so that the same fast and slow
	// paths are used consistently at all depths.
	return e.encodeValue(
		reflect.ValueOf(&data).Elem(),
		"root",
	)
}

func init() {
	// Force the local time zone to be loaded once at startup to avoid
	// lazy initialization races affecting serialized time.Time values.
	_ = time.Local.String()
}

func (e *Encoder[T]) encodeSimple(data any) (err error, isSimple bool) {
	switch v := data.(type) {
	case uintptr:
		return e.msgpackEncoder.Encode(uint64(v)), true
	// primitive scalars
	case bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		string,
		[]byte:
		return e.msgpackEncoder.Encode(data), true
	default:
		return nil, false
	}
}

var binaryMarshalerType = reflect.TypeFor[encoding.BinaryMarshaler]()

// isMoType reports whether t is (or is a pointer to) a type from the samber/mo package.
// These types implement encoding.BinaryMarshaler via gob, which breaks privateencoding's gaurantees.
func isMoType(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.Kind() == reflect.Struct &&
		t.PkgPath() == "github.com/samber/mo"
}

func implementsBinaryMarshaler(v reflect.Value) (func() encoding.BinaryMarshaler, bool) {
	typ := v.Type()
	if !typ.Implements(binaryMarshalerType) {
		return nil, false
	} else if isMoType(typ) {
		return nil, false
	}
	return func() encoding.BinaryMarshaler {
		return v.Interface().(encoding.BinaryMarshaler)
	}, true
}

func (e *Encoder[T]) encodeInterface(v reflect.Value, path string) error {
	isNil := v.IsNil()
	if err := e.mustEncodeSimple(isNil); err != nil {
		return encodePathError(path+" == nil", err)
	} else if isNil {
		return nil
	}

	concreteValue := v.Elem()
	// Concrete values extracted from interfaces can be non-addressable.
	// Clone to an addressable value before attempting unsafe access.
	if !concreteValue.CanAddr() {
		addressableConcreteValue := reflect.New(concreteValue.Type()).Elem()
		addressableConcreteValue.Set(concreteValue)
		concreteValue = addressableConcreteValue
	}
	concreteValue = privateencodinginternal.UnsafeValue(concreteValue)
	typeName, ok := lookupRegisteredTypeName(concreteValue.Type())
	if !ok {
		return encodePathError(path+".(type)", fmt.Errorf(
			"%w: %s",
			errInterfaceTypeNotRegistered,
			concreteValue.Type().String(),
		))
	}

	if err := e.mustEncodeSimple(typeName); err != nil {
		return encodePathError(path+".(type)", err)
	}

	return e.encodeValue(
		concreteValue,
		path+".("+typeName+")",
	)
}

func (e *Encoder[T]) encodeValue(v reflect.Value, path string) error {
	uv := privateencodinginternal.UnsafeValue(v)

	// Ignore lock-like, non-copyable structures (e.g. sync.Mutex). These fields
	// are not part of logical state and their internal state can change
	// nondeterministically.
	if isNoCopyStructType(uv.Type()) {
		return nil
	}

	if getMarshaler, ok := implementsBinaryMarshaler(uv); ok {
		if uv.Kind() == reflect.Pointer {
			return e.encodeNillable(uv, path, func(v reflect.Value) error {
				data, err := getMarshaler().MarshalBinary()
				if err != nil {
					return encodePathError(path, err)
				}
				if err := e.mustEncodeSimple(len(data)); err != nil {
					return encodePathError(path+".len", err)
				} else if _, err := e.w.Write(data); err != nil {
					return encodePathError(path+".data", err)
				}
				return nil
			})
		}
		data, err := getMarshaler().MarshalBinary()
		if err != nil {
			return encodePathError(path, err)
		}
		if err := e.mustEncodeSimple(len(data)); err != nil {
			return encodePathError(path+".len", err)
		} else if _, err := e.w.Write(data); err != nil {
			return encodePathError(path+".data", err)
		}
		return nil
	}
	if uv.Kind() == reflect.Interface {
		return e.encodeInterface(uv, path)
	}

	// first try to encode through the fast binary encoder
	err, isSimple := e.encodeSimple(
		uv.Interface(),
	)
	if err != nil {
		return encodePathError(path, err)
	} else if isSimple {
		return nil
	}

	// fallback to slow reflection-based encoding if needed
	return e.encodeComplex(v, path)
}

func (e *Encoder[T]) mustEncodeSimple(v any) error {
	if err, isSimple := e.encodeSimple(v); err != nil {
		return err
	} else if !isSimple {
		panic("expected simple encoding for " + reflect.TypeOf(v).String())
	}
	return nil
}

func encodePathError(path string, err error) error {
	if err == nil {
		return nil
	}
	return pathError("encode", path, err)
}

func encodeComplexNumber[T complex64 | complex128](w io.Writer, n T) error {
	switch v := any(n).(type) {
	case complex64:
		// encode as two float32 values (real, imag)
		var buf [8]byte
		r := math.Float32bits(real(v))
		i := math.Float32bits(imag(v))
		endianness().PutUint32(buf[0:4], r)
		endianness().PutUint32(buf[4:8], i)
		_, err := w.Write(buf[:])
		return err
	case complex128:
		// encode as two float64 values (real, imag)
		var buf [16]byte
		r := math.Float64bits(real(v))
		i := math.Float64bits(imag(v))
		endianness().PutUint64(buf[0:8], r)
		endianness().PutUint64(buf[8:16], i)
		_, err := w.Write(buf[:])
		return err
	default:
		return fmt.Errorf("unsupported complex type")
	}
}

func (e *Encoder[T]) encodeNillable(v reflect.Value, path string, encode func(v reflect.Value) error) error {
	isNil := v.IsNil()
	if err := e.mustEncodeSimple(isNil); err != nil {
		return encodePathError(path+" == nil", err)
	} else if isNil {
		return nil
	}
	return encode(v)
}

var ErrUnsupportedType = errors.New("unsupported type")

func (e *Encoder[T]) encodeComplex(v reflect.Value, path string) error {
	switch v.Kind() {
	case reflect.Pointer:
		return e.encodeNillable(v, path, func(v reflect.Value) error {
			return e.encodeValue(
				v.Elem(), "(*"+path+")",
			)
		})
	case reflect.Array:
		for i := range v.Len() {
			if err := e.encodeValue(v.Index(i), path+"["+strconv.Itoa(i)+"]"); err != nil {
				return err
			}
		}
		return nil
	case reflect.Slice:
		return e.encodeNillable(v, path, func(v reflect.Value) error {
			l := v.Len()
			if err := e.mustEncodeSimple(l); err != nil {
				return encodePathError("len("+path+")", err)
			}
			for i := range l {
				if err := e.encodeValue(v.Index(i), path+"["+strconv.Itoa(i)+"]"); err != nil {
					return err
				}
			}
			return nil
		})
	case reflect.Map:
		return e.encodeNillable(v, path, func(v reflect.Value) error {
			if err := e.mustEncodeSimple(v.Len()); err != nil {
				return encodePathError("len("+path+")", err)
			}
			type sortableEntry struct {
				key, value reflect.Value
				encodedKey []byte
			}
			entries := make([]sortableEntry, 0, v.Len())
			i := 0
			for iter := v.MapRange(); iter.Next(); i++ {
				encodedKey := make([]byte, 0, 32)
				buf := bytes.NewBuffer(encodedKey)
				enc := NewEncoder[reflect.Value](buf)
				addressableKey := reflect.New(iter.Key().Type()).Elem()
				addressableKey.Set(iter.Key())
				if err := enc.encodeValue(addressableKey, path+"[key-"+strconv.Itoa(i)+"]"); err != nil {
					return err
				}
				entries = append(entries, sortableEntry{
					key:        addressableKey,
					value:      iter.Value(),
					encodedKey: buf.Bytes(),
				})
			}
			sort.Slice(entries, func(i, j int) bool {
				return bytes.Compare(entries[i].encodedKey, entries[j].encodedKey) < 0
			})
			for i, entry := range entries {
				_, err := e.w.Write(entry.encodedKey)
				if err != nil {
					return encodePathError(path+"[key-"+strconv.Itoa(i)+"]", err)
				}
				addressableValue := reflect.New(entry.value.Type()).Elem()
				addressableValue.Set(entry.value)
				// Use indexed path for values to avoid expensive formatting.
				if err := e.encodeValue(addressableValue, path+"[val-"+strconv.Itoa(i)+"]"); err != nil {
					return err
				}
			}
			return nil
		})
	case reflect.Struct:
		for i := range v.NumField() {
			fv := v.Field(i)
			if !v.Type().Field(i).IsExported() {
				fv = privateencodinginternal.UnsafeValue(fv)
			}
			n := v.Type().Field(i).Name
			if err := e.encodeValue(fv, path+"."+n); err != nil {
				return err
			}
		}
		return nil
	// handle cases where simple types are aliased, which makes them complex
	case reflect.String:
		return encodePathError(path, e.mustEncodeSimple(v.String()))
	case reflect.Bool:
		return encodePathError(path, e.mustEncodeSimple(v.Bool()))
	case reflect.Int:
		return encodePathError(path, e.mustEncodeSimple(int(v.Int())))
	case reflect.Int8:
		return encodePathError(path, e.mustEncodeSimple(int8(v.Int())))
	case reflect.Int16:
		return encodePathError(path, e.mustEncodeSimple(int16(v.Int())))
	case reflect.Int32:
		return encodePathError(path, e.mustEncodeSimple(int32(v.Int())))
	case reflect.Int64:
		return encodePathError(path, e.mustEncodeSimple(v.Int()))
	case reflect.Uint:
		return encodePathError(path, e.mustEncodeSimple(uint(v.Uint())))
	case reflect.Uint8:
		return encodePathError(path, e.mustEncodeSimple(uint8(v.Uint())))
	case reflect.Uint16:
		return encodePathError(path, e.mustEncodeSimple(uint16(v.Uint())))
	case reflect.Uint32:
		return encodePathError(path, e.mustEncodeSimple(uint32(v.Uint())))
	case reflect.Uint64:
		return encodePathError(path, e.mustEncodeSimple(v.Uint()))
	case reflect.Float32:
		return encodePathError(path, e.mustEncodeSimple(float32(v.Float())))
	case reflect.Float64:
		return encodePathError(path, e.mustEncodeSimple(v.Float()))
	case reflect.Complex64:
		return encodePathError(path, encodeComplexNumber(e.w, complex64(v.Complex())))
	case reflect.Complex128:
		return encodePathError(path, encodeComplexNumber(e.w, v.Complex()))
	case reflect.Uintptr:
		return encodePathError(path, e.mustEncodeSimple(uintptr(v.Uint())))
	default:
		return fmt.Errorf("%w: %s", ErrUnsupportedType, v.Kind())
	}
}
