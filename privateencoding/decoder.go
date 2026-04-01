package privateencoding

import (
	"encoding"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"strconv"

	"github.com/vmihailenco/msgpack/v5"

	privateencodinginternal "github.com/futura-platform/futura/privateencoding/internal"
)

// Decoder is used to deserialize values of type T from a binary format,
// it deserializes exported AND unexported fields of the type T.
type Decoder[T any] struct {
	r io.Reader
	// msgpackDecoder is used as a fast path for primitive leaf values
	// (ints, floats, bools, strings, []byte) at any depth.
	msgpackDecoder *msgpack.Decoder
}

func NewDecoder[T any](r io.Reader) *Decoder[T] {
	return &Decoder[T]{
		r:              r,
		msgpackDecoder: msgpack.NewDecoder(r),
	}
}

func (d *Decoder[T]) Decode() (T, error) {
	var v T
	err := d.decodeValue(
		reflect.ValueOf(&v).Elem(),
		"root",
	)
	return v, err
}

// decodeSimple attempts to decode primitive leaf values using msgpack at any depth.
func (d *Decoder[T]) decodeSimple(v any) (error, bool) {
	switch v.(type) {
	case *[]byte,
		*string,
		*bool,
		*int, *int8, *int16, *int32, *int64,
		*uint, *uint8, *uint16, *uint32, *uint64,
		*float32, *float64:
		return d.msgpackDecoder.Decode(v), true
	default:
		return nil, false
	}
}

func (d *Decoder[T]) mustDecodeSimple(v any) error {
	err, isSimple := d.decodeSimple(v)
	if err != nil {
		return err
	} else if !isSimple {
		panic("type is not simple: " + reflect.TypeOf(v).String())
	}
	return nil
}

var binaryUnmarshalerType = reflect.TypeFor[encoding.BinaryUnmarshaler]()

func implementsBinaryUnmarshaler(v reflect.Value) (func() encoding.BinaryUnmarshaler, bool) {
	typ := v.Type()
	if typ.Kind() == reflect.Pointer && typ.Implements(binaryUnmarshalerType) {
		if isMoType(typ) {
			return nil, false
		}
		return func() encoding.BinaryUnmarshaler {
			if v.IsNil() {
				v.Set(reflect.New(typ.Elem()))
			}
			return v.Interface().(encoding.BinaryUnmarshaler)
		}, true
	}
	if !v.CanAddr() || !v.Addr().Type().Implements(binaryUnmarshalerType) {
		return nil, false
	}
	// Only use an address-based binary unmarshaller for value fields when the
	// value type would also have been binary-marshaled by the encoder. This
	// keeps encode/decode symmetric for types like url.URL, whose pointer type
	// implements Binary(Un)Marshaler but whose value type does not.
	if !typ.Implements(binaryMarshalerType) {
		return nil, false
	} else if isMoType(typ) {
		return nil, false
	}
	return func() encoding.BinaryUnmarshaler {
		return v.Addr().Interface().(encoding.BinaryUnmarshaler)
	}, true
}

func (d *Decoder[T]) decodeInterface(v reflect.Value, path string) error {
	var isNil bool
	if err := d.mustDecodeSimple(&isNil); err != nil {
		return decodePathError(path+" == nil", err)
	}
	if isNil {
		v.Set(reflect.Zero(v.Type()))
		return nil
	}

	var typeName string
	if err := d.mustDecodeSimple(&typeName); err != nil {
		return decodePathError(path+".(type)", err)
	}
	registeredType, ok := lookupRegisteredType(typeName)
	if !ok {
		return decodePathError(path+".(type)", fmt.Errorf(
			"%w: %s",
			errInterfaceTypeNotRegistered,
			typeName,
		))
	}

	decodedValue := reflect.New(registeredType).Elem()
	if err := d.decodeValue(decodedValue, path+".("+typeName+")"); err != nil {
		return err
	}

	if !decodedValue.Type().AssignableTo(v.Type()) {
		return decodePathError(path, fmt.Errorf(
			"%w: %s -> %s",
			errInterfaceTypeMismatch,
			decodedValue.Type().String(),
			v.Type().String(),
		))
	}
	v.Set(decodedValue)
	return nil
}

func (d *Decoder[T]) decodeValue(v reflect.Value, path string) error {
	uv := privateencodinginternal.UnsafeValue(v)

	// Ignore lock-like, non-copyable structures (e.g. sync.Mutex). These fields
	// are not part of logical state and are intentionally not serialized.
	if isNoCopyStructType(uv.Type()) {
		if uv.CanSet() {
			uv.Set(reflect.Zero(uv.Type()))
		}
		return nil
	}
	if getUnmarshaler, ok := implementsBinaryUnmarshaler(uv); ok {
		decodeBinary := func() error {
			var size int
			if err := d.mustDecodeSimple(&size); err != nil {
				return decodePathError(path, err)
			}
			data := make([]byte, size)
			if _, err := io.ReadFull(d.r, data); err != nil {
				return decodePathError(path, err)
			}
			if err := getUnmarshaler().UnmarshalBinary(data); err != nil {
				return decodePathError(path, err)
			}
			return nil
		}
		if uv.Kind() == reflect.Pointer {
			return d.decodeNillable(uv, path, decodeBinary)
		}
		return decodeBinary()
	}
	if uv.Kind() == reflect.Interface {
		return d.decodeInterface(uv, path)
	}

	// first try to decode through the fast primitive decoder
	err, isSimple := d.decodeSimple(uv.Addr().Interface())
	if err != nil {
		return decodePathError(path, err)
	} else if isSimple {
		return nil
	}

	// fallback to slow reflection-based decoding if needed
	return d.decodeComplex(v, path)
}

func (d *Decoder[T]) decodeNillable(v reflect.Value, path string, decode func() error) error {
	var isNil bool
	if err := d.mustDecodeSimple(&isNil); err != nil {
		return decodePathError(fmt.Sprintf("%s == nil", path), err)
	}
	if isNil {
		v.Set(reflect.Zero(v.Type()))
		return nil
	}
	return decode()
}

func mustDecodeSimpleValue[T, D any](d *Decoder[D], v reflect.Value) error {
	var tmp T
	if err := d.mustDecodeSimple(&tmp); err != nil {
		return err
	}
	v.Set(reflect.ValueOf(tmp).Convert(v.Type()))
	return nil
}

func (d *Decoder[T]) decodeComplex(v reflect.Value, path string) error {
	switch v.Kind() {
	case reflect.Pointer:
		return d.decodeNillable(v, path, func() error {
			newValue := reflect.New(v.Type().Elem())
			v.Set(newValue)
			return d.decodeValue(
				newValue.Elem(),
				"(*"+path+")",
			)
		})
	case reflect.Slice:
		return d.decodeNillable(v, path, func() error {
			var l int
			if err := d.mustDecodeSimple(&l); err != nil {
				return decodePathError("len("+path+")", err)
			}
			v.Set(reflect.MakeSlice(v.Type(), l, l))
			for i := range l {
				err := d.decodeValue(v.Index(i), path+"["+strconv.Itoa(i)+"]")
				if err != nil {
					return err
				}
			}
			return nil
		})
	case reflect.Map:
		return d.decodeNillable(v, path, func() error {
			var l int
			if err := d.mustDecodeSimple(&l); err != nil {
				return decodePathError("len("+path+")", err)
			}
			v.Set(reflect.MakeMapWithSize(v.Type(), l))
			for i := range l {
				key := reflect.New(v.Type().Key()).Elem()
				err := d.decodeValue(
					key,
					path+"[key-"+strconv.Itoa(i)+"]",
				)
				if err != nil {
					return err
				}
				value := reflect.New(v.Type().Elem()).Elem()
				err = d.decodeValue(value, path+"["+key.String()+"]")
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
			err := d.decodeValue(fv, path+"."+field.Name)
			if err != nil {
				return err
			}
		}
		return nil

	// handle cases where simple types are aliased, which makes them complex
	case reflect.String:
		if err := mustDecodeSimpleValue[string](d, v); err != nil {
			return decodePathError(path, err)
		}
		return nil
	case reflect.Bool:
		if err := mustDecodeSimpleValue[bool](d, v); err != nil {
			return decodePathError(path, err)
		}
		return nil
	case reflect.Int:
		if err := mustDecodeSimpleValue[int64](d, v); err != nil {
			return decodePathError(path, err)
		}
		return nil
	case reflect.Uint:
		if err := mustDecodeSimpleValue[uint64](d, v); err != nil {
			return decodePathError(path, err)
		}
		return nil
	case reflect.Int8:
		if err := mustDecodeSimpleValue[int8](d, v); err != nil {
			return decodePathError(path, err)
		}
		return nil
	case reflect.Uint8:
		if err := mustDecodeSimpleValue[uint8](d, v); err != nil {
			return decodePathError(path, err)
		}
		return nil
	case reflect.Int16:
		if err := mustDecodeSimpleValue[int16](d, v); err != nil {
			return decodePathError(path, err)
		}
		return nil
	case reflect.Uint16:
		if err := mustDecodeSimpleValue[uint16](d, v); err != nil {
			return decodePathError(path, err)
		}
		return nil
	case reflect.Int32:
		if err := mustDecodeSimpleValue[int32](d, v); err != nil {
			return decodePathError(path, err)
		}
		return nil
	case reflect.Uint32:
		if err := mustDecodeSimpleValue[uint32](d, v); err != nil {
			return decodePathError(path, err)
		}
		return nil
	case reflect.Int64:
		if err := mustDecodeSimpleValue[int64](d, v); err != nil {
			return decodePathError(path, err)
		}
		return nil
	case reflect.Uint64:
		if err := mustDecodeSimpleValue[uint64](d, v); err != nil {
			return decodePathError(path, err)
		}
		return nil
	case reflect.Float32:
		if err := mustDecodeSimpleValue[float32](d, v); err != nil {
			return decodePathError(path, err)
		}
		return nil
	case reflect.Float64:
		if err := mustDecodeSimpleValue[float64](d, v); err != nil {
			return decodePathError(path, err)
		}
		return nil
	case reflect.Complex64, reflect.Complex128:
		var buf [16]byte
		size := int(v.Type().Size())
		if _, err := io.ReadFull(d.r, buf[:size]); err != nil {
			return decodePathError(path, err)
		}
		_, err := binary.Decode(buf[:size], endianness(), v.Addr().Interface())
		if err != nil {
			return decodePathError(path, err)
		}
		return nil
	case reflect.Uintptr:
		if err := mustDecodeSimpleValue[uint64](d, v); err != nil {
			return decodePathError(path, err)
		}
		return nil
	default:
		return fmt.Errorf("unsupported type: %s", v.Kind())
	}
}

func decodePathError(path string, err error) error {
	return pathError("decode", path, err)
}
