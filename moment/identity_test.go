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
		assert.Equal(t, []string{"placeholder"}, key)
	})
	t.Run("multiple layer keys", func(t *testing.T) {
		ctx := t.Context()
		ctx = WithIdentityKey(ctx, "placeholder")
		ctx = WithIdentityKey(ctx, "placeholder2")
		key, ok := IdentityFromContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, []string{"placeholder", "placeholder2"}, key)
	})
	t.Run("layered keys are distinct from a single key with the same characters", func(t *testing.T) {
		callpath := Callpath{{File: "a.go", Line: 1}}
		layered := NewIdentity(WithIdentityKey(WithIdentityKey(t.Context(), "a-b"), "c"), callpath)
		alsoLayered := NewIdentity(WithIdentityKey(WithIdentityKey(t.Context(), "a"), "b-c"), callpath)
		single := NewIdentity(WithIdentityKey(t.Context(), "a-b-c"), callpath)
		assert.NotEqual(t, layered, alsoLayered)
		assert.NotEqual(t, layered, single)
		assert.NotEqual(t, alsoLayered, single)
	})
	t.Run("keys with any content stay distinct", func(t *testing.T) {
		callpath := Callpath{{File: "a.go", Line: 1}}
		build := func(keys ...string) Identity {
			ctx := t.Context()
			for _, k := range keys {
				ctx = WithIdentityKey(ctx, k)
			}
			return NewIdentity(ctx, callpath)
		}
		assert.NotEqual(t, build("a b"), build("a", "b"))
		assert.NotEqual(t, build("[a]"), build("a"))
		assert.NotEqual(t, build(`"a"`), build("a"))
		assert.NotEqual(t, build("a", ""), build("a"))
		assert.NotEqual(t, build(""), build())
	})
}

func TestNewIdentity(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		ctx := t.Context()
		ctx = WithIdentityKey(ctx, "placeholder")

		callpath := Callpath{{File: "placeholder-path"}}
		identity := NewIdentity(ctx, callpath)

		assert.Equal(t, []string{"placeholder"}, identity.key.V())
		assert.Equal(t, callpath, identity.Callpath().V())
	})
}
