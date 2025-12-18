package ftype

import (
	"bytes"
	"strings"
	"unsafe"

	"github.com/futura-platform/futura/internal/privateencoding"
)

type Sealed[T any] interface {
	V() *T
}

type sealedWithString[T any] struct {
	// The serialized value of the input value.
	// It is a string so that it is comparable.
	comparableSerialized string
}

// Seal creates a read only, "sealed" value for any input value.
// This is a way to make any value comparable+memoizable.
// Exported and unexported values are serializable.
func Seal[T any](value T) Sealed[T] {
	sizeHeuristic := unsafe.Sizeof(value)
	buf := bytes.NewBuffer(make([]byte, 0, sizeHeuristic))

	enc := privateencoding.NewEncoder[T](buf)
	if err := enc.Encode(value); err != nil {
		panic(err)
	}

	return &sealedWithString[T]{
		comparableSerialized: buf.String(),
	}
}

// V returns a pointer to the underlying value of the sealed value.
// The return value is guaranteed to have the same shape
// as the input value, but it is not guaranteed to have the same pointers.
// This value should be treated as a copy of the input value.
// This value should be treated as immutable. If it is mutated within a Step,
// The Step will panic (if running in debug mode).
func (s *sealedWithString[T]) V() *T {
	dec := privateencoding.NewDecoder[T](strings.NewReader(s.comparableSerialized))
	value, err := dec.Decode()
	if err != nil {
		panic(err)
	}
	return &value
}
