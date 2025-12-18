package privateencoding

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"reflect"

	privateencodinginternal "github.com/futura-platform/futura/internal/privateencoding/internal"
)

// Decoder is used to deserialize values of type T from a binary format,
// it deserializes exported AND unexported fields of the type T.
type Decoder[T any] struct {
	r                io.Reader
	interfaceDecoder interface {
		DecodeValue(v reflect.Value) error
	}
}

func NewDecoder[T any](r io.Reader) *Decoder[T] {
	return &Decoder[T]{r: r, interfaceDecoder: gob.NewDecoder(r)}
}

func (d *Decoder[T]) Decode() (T, error) {
	v := new(T)
	return *v, d.decodeValue(reflect.ValueOf(v).Elem(), "root")
}

func decodeSimple(r io.Reader, v any) (error, bool) {
	readBytes := func() ([]byte, error) {
		sizeBuf := make([]byte, 8)
		_, err := r.Read(sizeBuf)
		if err != nil {
			return nil, err
		}
		size := endianness().Uint64(sizeBuf)
		if size == 0 {
			return []byte{}, nil
		}
		buf := make([]byte, size)
		_, err = r.Read(buf)
		return buf, err
	}

	// first check primitive types that don't have a fixed size
	switch original := v.(type) {
	case *[]byte:
		isNil, err := readSimple[bool](r)
		if err != nil {
			return err, false
		} else if isNil {
			*original = nil
			return nil, true
		}

		bytes, err := readBytes()
		if err != nil {
			return err, false
		}
		*original = bytes
		return nil, true
	case *string:
		bytes, err := readBytes()
		if err != nil {
			return err, false
		}
		*original = string(bytes)
		return nil, true

	case *int:
		v = new(int64)
		defer func() {
			if original == nil {
				fmt.Println("original is nil")
			}
			*original = int(*(v.(*int64)))
		}()
	case *uint:
		v = new(uint64)
		defer func() {
			*original = uint(*(v.(*uint64)))
		}()
	}

	var size int
	switch v.(type) {
	case *int64, *uint64, *float64:
		size = 8
	case *int32, *uint32, *float32:
		size = 4
	case *int16, *uint16:
		size = 2
	case *int8, *uint8, *bool:
		size = 1
	default:
		return nil, false
	}
	buf := make([]byte, size)
	_, err := r.Read(buf)
	if err != nil {
		return err, false
	}
	_, err = binary.Decode(buf, endianness(), v)
	if err != nil {
		return err, false
	}
	return nil, true
}

func readSimple[T any](r io.Reader) (T, error) {
	var v T
	err := mustDecodeSimple(r, &v)
	if err != nil {
		return v, err
	}
	return v, nil
}

func mustDecodeSimple(r io.Reader, v any) error {
	err, isSimple := decodeSimple(r, v)
	if err != nil {
		return err
	} else if !isSimple {
		panic(fmt.Sprintf("type is not simple: %T", v))
	}
	return nil
}

func mustDecodeSimpleValue[T any](r io.Reader, v reflect.Value) error {
	vv := new(T)
	err := mustDecodeSimple(r, vv)

	rv := reflect.ValueOf(*vv)
	v.Set(rv.Convert(v.Type()))
	return err
}

func (d *Decoder[T]) decodeValue(v reflect.Value, path string) error {
	uv := privateencodinginternal.UnsafeValue(v)
	if uv.Kind() == reflect.Interface {
		return decodePathError(path, d.interfaceDecoder.DecodeValue(uv))
	}

	// first try to decode through the fast binary decoder
	err, isSimple := decodeSimple(d.r, uv.Addr().Interface())
	if err != nil {
		return decodePathError(path, err)
	} else if isSimple {
		return nil
	}

	// fallback to slow reflection-based decoding if needed
	return d.decodeComplex(v, path)
}

func (d *Decoder[T]) decodeNillable(v reflect.Value, path string, decode func() error) error {
	isNil, err := readSimple[bool](d.r)
	if err != nil {
		return decodePathError(fmt.Sprintf("%s == nil", path), err)
	} else if isNil {
		v.Set(reflect.Zero(v.Type()))
		return nil
	}

	return decode()
}

func (d *Decoder[T]) decodeComplex(v reflect.Value, path string) error {
	if !v.CanAddr() {
		panic(fmt.Sprintf("not addressable: %s", v.Type()))
	}
	switch v.Kind() {
	case reflect.Pointer:
		return d.decodeNillable(v, path, func() error {
			if path == "root.Direct.loc" {
				fmt.Println("debug")
			}
			newValue := reflect.New(v.Type().Elem())
			v.Set(newValue)
			return d.decodeValue(
				newValue.Elem(),
				fmt.Sprintf("(*%s)", path),
			)
		})
	case reflect.Slice:
		return d.decodeNillable(v, path, func() error {
			len, err := readSimple[int](d.r)
			if err != nil {
				return decodePathError(fmt.Sprintf("len(%s)", path), err)
			}
			v.Set(reflect.MakeSlice(v.Type(), len, len))
			for i := range len {
				err := d.decodeValue(v.Index(i), fmt.Sprintf("%s[%d]", path, i))
				if err != nil {
					return err
				}
			}
			return nil
		})
	case reflect.Map:
		return d.decodeNillable(v, path, func() error {
			len, err := readSimple[int](d.r)
			if err != nil {
				return decodePathError(fmt.Sprintf("len(%s)", path), err)
			}
			v.Set(reflect.MakeMapWithSize(v.Type(), len))
			for i := range len {
				key := reflect.New(v.Type().Key()).Elem()
				err := d.decodeValue(
					key,
					fmt.Sprintf("%s[key-%d]", path, i),
				)
				if err != nil {
					return err
				}
				value := reflect.New(v.Type().Elem()).Elem()
				err = d.decodeValue(value, fmt.Sprintf("%s[%s]", path, key.String()))
				if err != nil {
					return err
				}
				v.SetMapIndex(key, value)
			}
			return nil
		})
	case reflect.Struct:
		for i := range v.NumField() {
			field := v.Type().Field(i)
			fv := v.Field(i)
			if !field.IsExported() {
				fv = privateencodinginternal.UnsafeValue(fv)
			}
			err := d.decodeValue(fv, fmt.Sprintf("%s.%s", path, field.Name))
			if err != nil {
				return err
			}
		}
		return nil

	// handle cases where simple types are aliased, which makes them complex
	case reflect.String:
		return decodePathError(path, mustDecodeSimpleValue[string](d.r, v))
	case reflect.Bool:
		return decodePathError(path, mustDecodeSimpleValue[bool](d.r, v))
	case reflect.Int:
		return decodePathError(path, mustDecodeSimpleValue[int64](d.r, v))
	case reflect.Uint:
		return decodePathError(path, mustDecodeSimpleValue[uint64](d.r, v))
	case reflect.Int8:
		return decodePathError(path, mustDecodeSimpleValue[int8](d.r, v))
	case reflect.Uint8:
		return decodePathError(path, mustDecodeSimpleValue[uint8](d.r, v))
	case reflect.Int16:
		return decodePathError(path, mustDecodeSimpleValue[int16](d.r, v))
	case reflect.Uint16:
		return decodePathError(path, mustDecodeSimpleValue[uint16](d.r, v))
	case reflect.Int32:
		return decodePathError(path, mustDecodeSimpleValue[int32](d.r, v))
	case reflect.Uint32:
		return decodePathError(path, mustDecodeSimpleValue[uint32](d.r, v))
	case reflect.Int64:
		return decodePathError(path, mustDecodeSimpleValue[int64](d.r, v))
	case reflect.Uint64:
		return decodePathError(path, mustDecodeSimpleValue[uint64](d.r, v))
	case reflect.Float32:
		return decodePathError(path, mustDecodeSimpleValue[float32](d.r, v))
	case reflect.Float64:
		return decodePathError(path, mustDecodeSimpleValue[float64](d.r, v))
	case reflect.Complex64, reflect.Complex128:
		buf := make([]byte, v.Type().Size())
		_, err := d.r.Read(buf)
		if err != nil {
			return decodePathError(path, err)
		}
		_, err = binary.Decode(buf, endianness(), v.Addr().Interface())
		return decodePathError(path, err)
	case reflect.Uintptr:
		return decodePathError(path, mustDecodeSimpleValue[uint64](d.r, v))
	default:
		return fmt.Errorf("unsupported type: %s", v.Kind())
	}
}

func decodePathError(path string, err error) error {
	if err == nil {
		return nil
	}
	return pathError("decode", path, err)
}
