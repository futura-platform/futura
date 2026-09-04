package privateencoding_test

import (
	"encoding"
	"testing"
	"time"

	"github.com/futura-platform/futura/privateencoding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type withNilMarshaler struct {
	N int
	M encoding.BinaryMarshaler
	T *time.Time
}

type marshalerField struct {
	Body encoding.BinaryMarshaler
}

func TestEncoder_NilMarshalerEncodes(t *testing.T) {
	decoded := roundTrip(t, withNilMarshaler{N: 1})
	assert.Equal(t, 1, decoded.N)
	assert.Nil(t, decoded.M)
	assert.Nil(t, decoded.T)
}

func TestEncoder_MarshalerTypedInterfaceFieldRoundTrips(t *testing.T) {
	privateencoding.Register[time.Time]()
	t0 := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	decoded := roundTrip(t, marshalerField{Body: t0})
	require.NotNil(t, decoded.Body)
	assert.True(t, t0.Equal(decoded.Body.(time.Time)))
	decoded = roundTrip(t, marshalerField{})
	assert.Nil(t, decoded.Body)
}

// unmarshalOnly can load itself from an external blob but cannot produce one: it must be encoded and
// decoded field by field, never through the half of the pair it happens to have.
type unmarshalOnly struct {
	Attempts uint8
	Token    [3]uint8
}

func (s *unmarshalOnly) UnmarshalBinary([]byte) error {
	s.Attempts = 200
	return nil
}

// marshalOnly is the mirror: it can produce a blob but not load one.
type marshalOnly struct {
	N int
}

func (marshalOnly) MarshalBinary() ([]byte, error) { return []byte{0xff}, nil }

func TestEncoder_BinaryFormNeedsBothHalves(t *testing.T) {
	// the encoder and the decoder must agree on the wire form from the type alone, so a type with only
	// one half of the pair takes the structural form on both sides
	t.Run("unmarshal only", func(t *testing.T) {
		v := &unmarshalOnly{Attempts: 3, Token: [3]uint8{7, 8, 9}}
		assert.Equal(t, v, roundTrip(t, v))
		assert.Equal(t, *v, roundTrip(t, *v))
	})
	t.Run("marshal only", func(t *testing.T) {
		v := &marshalOnly{N: 42}
		assert.Equal(t, v, roundTrip(t, v))
		assert.Equal(t, *v, roundTrip(t, *v))
		assert.NotContains(t, encodeValue(t, v), byte(0xff), "the lone MarshalBinary was used")
	})
}
