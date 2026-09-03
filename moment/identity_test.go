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
		callpath, fn := Callpath{{File: "a.go", Line: 1}}, Callsite{File: "a.go", Line: 10}
		layered := NewIdentity(WithIdentityKey(WithIdentityKey(t.Context(), "a-b"), "c"), callpath, fn)
		alsoLayered := NewIdentity(WithIdentityKey(WithIdentityKey(t.Context(), "a"), "b-c"), callpath, fn)
		single := NewIdentity(WithIdentityKey(t.Context(), "a-b-c"), callpath, fn)
		assert.NotEqual(t, layered, alsoLayered)
		assert.NotEqual(t, layered, single)
		assert.NotEqual(t, alsoLayered, single)
	})
	t.Run("keys with any content stay distinct", func(t *testing.T) {
		callpath, fn := Callpath{{File: "a.go", Line: 1}}, Callsite{File: "a.go", Line: 10}
		build := func(keys ...string) Identity {
			ctx := t.Context()
			for _, k := range keys {
				ctx = WithIdentityKey(ctx, k)
			}
			return NewIdentity(ctx, callpath, fn)
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
		fn := Callsite{File: "placeholder-path", Line: 10}
		identity := NewIdentity(ctx, callpath, fn)

		assert.Equal(t, []string{"placeholder"}, identity.key.V())
		assert.Equal(t, callpath, identity.Callpath().V())
		assert.Equal(t, fn, identity.fn)
	})
	t.Run("the same callpath reached with different functions gives different identities", func(t *testing.T) {
		callpath := Callpath{{File: "a.go", Line: 1}}
		first := NewIdentity(t.Context(), callpath, Callsite{File: "a.go", Line: 10})
		second := NewIdentity(t.Context(), callpath, Callsite{File: "a.go", Line: 20})
		assert.NotEqual(t, first, second)
	})
}
