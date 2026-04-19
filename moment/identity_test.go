package moment

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithIdentityKey(t *testing.T) {
	t.Run("single layer key", func(t *testing.T) {
		ctx := t.Context()
		ctx = WithIdentityKey(ctx, "placeholder")
		key, ok := IdentityFromContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, "placeholder", key)
	})
	t.Run("multiple layer keys", func(t *testing.T) {
		ctx := t.Context()
		ctx = WithIdentityKey(ctx, "placeholder")
		ctx = WithIdentityKey(ctx, "placeholder2")
		key, ok := IdentityFromContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, "placeholder-placeholder2", key)
	})
}

func TestNewIdentity(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		ctx := t.Context()
		ctx = WithIdentityKey(ctx, "placeholder")

		callpath := Callpath{{File: "placeholder-path"}}
		identity := NewIdentity(ctx, callpath)

		assert.Equal(t, "placeholder", identity.key)
		assert.Equal(t, callpath, identity.Callpath().V())
	})
}
