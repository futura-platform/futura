package privateencoding_test

import (
	"bytes"
	"testing"

	"github.com/futura-platform/futura/privateencoding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestCodec_InterfaceTypeRegistration(t *testing.T) {
	t.Run("encode_fails_if_type_is_unregistered", func(t *testing.T) {
		type unregisteredPayload struct {
			value int
		}

		_, err := applyCodec(any(unregisteredPayload{value: 1}))
		require.Error(t, err)
		assert.ErrorContains(t, err, "interface type not registered")
	})

	t.Run("decode_fails_if_type_name_is_unknown", func(t *testing.T) {
		buf := bytes.NewBuffer(nil)
		enc := msgpack.NewEncoder(buf)
		require.NoError(t, enc.Encode(false))
		require.NoError(t, enc.Encode("github.com/futura-platform/futura/privateencoding_test.unknownType"))

		dec := privateencoding.NewDecoder[any](buf)
		_, err := dec.Decode()
		require.Error(t, err)
		assert.ErrorContains(t, err, "interface type not registered")
	})
}
