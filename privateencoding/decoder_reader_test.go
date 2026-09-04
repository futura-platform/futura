package privateencoding_test

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/futura-platform/futura/privateencoding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type readOnly struct{ r io.Reader }

func (r readOnly) Read(p []byte) (int, error) { return r.r.Read(p) }

func TestDecoder_PlainReader(t *testing.T) {
	// msgpack buffers ahead on a reader that cannot unread; the raw reads that follow must see the same stream
	type mixed struct {
		N int
		T time.Time
		C complex128
		S string
	}
	v := mixed{N: 1, T: time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC), C: 2 + 3i, S: "after"}
	data := encodeValue(t, v)
	decoded, err := privateencoding.NewDecoder[mixed](readOnly{bytes.NewReader(data)}).Decode()
	require.NoError(t, err)
	assert.Equal(t, v.N, decoded.N)
	assert.True(t, v.T.Equal(decoded.T))
	assert.Equal(t, v.C, decoded.C)
	assert.Equal(t, v.S, decoded.S)
}
