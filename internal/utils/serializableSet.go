package utils

import (
	"bytes"
	"encoding"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/futura/privateencoding"
)

type SerializableSet[T comparable] struct {
	mapset.Set[T]
}

var _ encoding.BinaryMarshaler = SerializableSet[any]{}
var _ encoding.BinaryUnmarshaler = &SerializableSet[any]{}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (s *SerializableSet[T]) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)
	decoder := privateencoding.NewDecoder[[]T](buf)
	vals, err := decoder.Decode()
	if err != nil {
		return err
	}
	s.Set = mapset.NewSet(vals...)
	return nil
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (s SerializableSet[T]) MarshalBinary() (data []byte, err error) {
	buf := bytes.NewBuffer(nil)
	encoder := privateencoding.NewEncoder[[]T](buf)
	err = encoder.Encode(s.ToSlice())
	return buf.Bytes(), err
}

func NewSerializableSet[T comparable](set mapset.Set[T]) SerializableSet[T] {
	return SerializableSet[T]{set}
}
