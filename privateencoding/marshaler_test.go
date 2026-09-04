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
