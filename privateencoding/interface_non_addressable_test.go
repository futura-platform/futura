package privateencoding_test

import (
	"testing"

	"github.com/futura-platform/futura/ftype/seal"
	"github.com/futura-platform/futura/moment"
	"github.com/futura-platform/futura/privateencoding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type interfaceConcreteWithUnexportedFields struct {
	hidden int
}

type interfaceValueContainer struct {
	value any
}

type sealedResult struct {
	ID int
}

func TestCodec_InterfaceEncoding_WithNonAddressableConcreteValues(t *testing.T) {
	t.Run("interface concrete type has unexported fields", func(t *testing.T) {
		privateencoding.Register[interfaceConcreteWithUnexportedFields]()

		input := interfaceValueContainer{
			value: interfaceConcreteWithUnexportedFields{hidden: 42},
		}

		var (
			decoded interfaceValueContainer
			err     error
		)

		assert.NotPanics(t, func() {
			decoded, err = applyCodec(input)
		})
		require.NoError(t, err)
		assert.Equal(t, input, decoded)
	})

	t.Run("sealed value encoded behind any in moment-like struct", func(t *testing.T) {
		privateencoding.Register[seal.Sealed[[]sealedResult]]()

		m := moment.Moment{}
		m.SetValidOutput(seal.Seal([]sealedResult{
			{ID: 1},
			{ID: 2},
		}))

		var (
			decoded moment.Moment
			err     error
		)

		assert.NotPanics(t, func() {
			decoded, err = applyCodec(m)
		})
		require.NoError(t, err)
		assert.Equal(t, m.Output(), decoded.Output())
		assert.Equal(t, m, decoded)
	})

	t.Run("encoding is deterministic for moment-like value with sealed any output", func(t *testing.T) {
		privateencoding.Register[seal.Sealed[[]sealedResult]]()

		m := moment.Moment{}
		m.SetValidOutput(seal.Seal([]sealedResult{
			{ID: 9},
			{ID: 10},
		}))

		first := encodeValue(t, m)
		second := encodeValue(t, m)
		assert.Equal(t, first, second)

		decoded, err := applyCodec(m)
		require.NoError(t, err)
		assert.Equal(t, m.Output(), decoded.Output())
	})
}
