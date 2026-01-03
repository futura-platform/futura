package privateencoding

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	privateencodinginternal "github.com/futura-platform/futura/internal/privateencoding/internal"
)

// Encoder is used to serialize values of type T to a binary format,
// it serializes exported AND unexported fields of the type T.
type Encoder[T any] struct {
	w                io.Writer
	interfaceEncoder interface {
		EncodeValue(v reflect.Value) error
	}
	// msgpackEncoder is used as a fast path for primitive leaf values
	// (ints, floats, bools, strings, []byte) at any depth.
	msgpackEncoder *msgpack.Encoder
}

func NewEncoder[T any](w io.Writer) *Encoder[T] {
	return &Encoder[T]{
		w:                w,
		interfaceEncoder: gob.NewEncoder(w),
		msgpackEncoder:   msgpack.NewEncoder(w),
	}
}

func (e *Encoder[T]) Encode(data T) error {
	// Always go through encodeValue so that the same fast and slow
	// paths are used consistently at all depths.
	return e.encodeValue(reflect.ValueOf(&data).Elem(), "root")
}

type Kind byte

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

func (e *Encoder[T]) encodeValue(v reflect.Value, path string) error {
	uv := privateencodinginternal.UnsafeValue(v)
	if uv.Kind() == reflect.Interface {
		// let gob handle interface serialization
		return encodePathError(path, e.interfaceEncoder.EncodeValue(uv))
	}

	// first try to encode through the fast binary encoder
	err, isSimple := e.encodeSimple(uv.Interface())
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
			i := 0
			for iter := v.MapRange(); iter.Next(); i++ {
				iterKey := iter.Key()
				addressableKey := reflect.New(iterKey.Type()).Elem()
				addressableKey.Set(iterKey)
				if err := e.encodeValue(addressableKey, path+"[key-"+strconv.Itoa(i)+"]"); err != nil {
					return err
				}
				iterValue := iter.Value()
				addressableValue := reflect.New(iterValue.Type()).Elem()
				addressableValue.Set(iterValue)
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
