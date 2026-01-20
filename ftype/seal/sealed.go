package seal

import (
	"bytes"
	"fmt"
	"strings"
	"unsafe"

	"github.com/futura-platform/futura/privateencoding"
)

type Sealed[T any] struct {
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

	return Sealed[T]{
		comparableSerialized: buf.String(),
	}
}

// V returns the underlying value of the sealed value.
// The return value is guaranteed to have the same shape
// as the input value, but it is not guaranteed to have the same pointers.
func (s Sealed[T]) V() T {
	reader := strings.NewReader(s.comparableSerialized)
	dec := privateencoding.NewDecoder[T](reader)
	value, err := dec.Decode()
	if err != nil {
		panic(fmt.Errorf("failed to decode sealed value: %w", err))
	} else if reader.Len() != 0 {
		panic(fmt.Errorf("expected reader to be at the end, still have remaining bytes: %d", reader.Len()))
	}
	return value
}
