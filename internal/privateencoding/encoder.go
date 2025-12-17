package privateencoding

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"reflect"

	privateencodinginternal "github.com/futura-platform/futura/internal/privateencoding/internal"
)

// Encoder is used to serialize values of type T to a binary format,
// it serializes exported AND unexported fields of the type T.
type Encoder[T any] struct {
	w                io.Writer
	interfaceEncoder interface {
		EncodeValue(v reflect.Value) error
	}
}

func NewEncoder[T any](w io.Writer) *Encoder[T] {
	return &Encoder[T]{w: w, interfaceEncoder: gob.NewEncoder(w)}
}

type Kind byte

func (e *Encoder[T]) Encode(data T) error {
	return e.encodeValue(
		// get the address of the data and then the element of the pointer
		// so that it is addressable and we can access the unexported fields
		reflect.ValueOf(&data).Elem(), "root")
}

func (e *Encoder[T]) encodeSimple(data any) (err error, isSimple bool) {
	writeBytes := func(b []byte) error {
		buf := append(make([]byte, 8), b...)
		endianness().PutUint64(buf, uint64(len(b)))
		return e.write(buf)
	}
	// first check primitive types that don't have a fixed size
	switch v := data.(type) {
	case []byte:
		return writeBytes(v), true
	case string:
		return writeBytes([]byte(v)), true
	// treat system-native integer types as 64 bit
	case int:
		data = int64(v)
	case uint:
		data = uint64(v)
	case uintptr:
		data = uint64(v)
	}

	// first check if we can use encoding/binary's fast path for primitive types
	intSize := intDataSize(data)
	if intSize != 0 {
		buf := make([]byte, intSize)
		_, err := binary.Encode(buf, endianness(), data)
		if err != nil {
			return err, false
		}
		return e.write(buf), true
	}

	return nil, false
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
		panic(fmt.Sprintf("expected simple encoding for %T", v))
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
	var buf []byte
	switch any(n).(type) {
	case complex64:
		buf = make([]byte, 8)
	case complex128:
		buf = make([]byte, 16)
	}
	_, err := binary.Encode(buf, endianness(), n)
	if err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

func (e *Encoder[T]) encodeComplex(v reflect.Value, path string) error {
	switch v.Kind() {
	case reflect.Pointer:
		return e.encodeValue(v.Elem(), fmt.Sprintf("(*%s)", path))
	case reflect.Slice:
		l := v.Len()
		if err := e.mustEncodeSimple(l); err != nil {
			return encodePathError(fmt.Sprintf("len(%s)", path), err)
		}
		for i := range l {
			if err := e.encodeValue(v.Index(i), fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
		return nil
	case reflect.Map:
		if err := e.mustEncodeSimple(v.Len()); err != nil {
			return encodePathError(fmt.Sprintf("len(%s)", path), err)
		}
		i := 0
		for iter := v.MapRange(); iter.Next(); i++ {
			if err := e.encodeValue(iter.Key(), fmt.Sprintf("%s[key-%d]", path, i)); err != nil {
				return err
			}
			if err := e.encodeValue(iter.Value(), fmt.Sprintf("%s[%v]", path, iter.Value().Interface())); err != nil {
				return err
			}
		}
		return nil
	case reflect.Struct:
		for i := range v.NumField() {
			fv := v.Field(i)
			if !v.Type().Field(i).IsExported() {
				fv = privateencodinginternal.UnsafeValue(fv)
			}
			n := v.Type().Field(i).Name
			if err := e.encodeValue(fv, fmt.Sprintf("%s.%s", path, n)); err != nil {
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
		return fmt.Errorf("unsupported type: %s", v.Kind())
	}
}

func (e *Encoder[T]) write(value []byte) error {
	_, err := e.w.Write(value)
	return err
}
