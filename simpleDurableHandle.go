package futura

import (
	"bytes"

	"github.com/futura-platform/futura/privateencoding"
)

// NewPlainDurableHandle creates a new durable handle for a "plain" type T.
// A plain type is a type that can be safely serialized and deserialized without needing any special handling,
// and does not require any cleanup. This function is a convenience wrapper around NewDurableHandle that provides
// default unmarshal and marshal functions that use privateencoding, which serialize and deserialize an entire object,
// including its exported AND UNexported fields.
func NewPlainDurableHandle[T any](
	key string,
	constructor func() *T,
) *DurableHandle[T] {
	return NewDurableHandle[T](
		key,
		constructor,
		func(b []byte) (*T, error) {
			decoder := privateencoding.NewDecoder[*T](bytes.NewReader(b))
			return decoder.Decode()
		},
		func(v *T) ([]byte, error) {
			buf := bytes.NewBuffer(nil)
			encoder := privateencoding.NewEncoder[*T](buf)
			if err := encoder.Encode(v); err != nil {
				return nil, err
			}
			return buf.Bytes(), nil
		},
		nil,
	)
}
