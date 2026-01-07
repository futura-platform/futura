package moment

import (
	"context"
	"fmt"

	"github.com/futura-platform/futura/ftype/seal"
)

// Identity is a unique identifier for a moment.
// It is used to identify the specific point in time a moment occurs in the flow.
// The callpath identifies where in the code the moment is defined, and the key identifies the "instance" of the moment.
//
// This is useful for moments produced by loops. This is a similar concept to React's "key" prop.
// "key"s much be comparable, this is enforced in the WithMomentIdentityKey function.
//
// "key" is assigned through context, so that api consumers can have the ability to encapsulate logic that doesn't need to know about the key.
// (like a helper function that could be called inside or out of a loop)
type Identity struct {
	callpath seal.Sealed[Callpath]
	key      any
}

func (i Identity) Callpath() seal.Sealed[Callpath] {
	return i.callpath
}

func NewIdentity(ctx context.Context, callpath Callpath) Identity {
	return Identity{
		callpath: seal.Seal(callpath),
		key:      IdentityFromContext(ctx),
	}
}

func (i Identity) String() string {
	return fmt.Sprintf("key:%v callpath:%s", i.key, i.callpath.V())
}

type contextKey string

const ContextKey contextKey = "futura_moment_identity_key"

// we need a way to stack identity keys, so that WithIdentityKey can be called multiple times to layer keys.
// we need the structure that stores this stack to be comparable.
// This means we must use a self referencing struct instead of a slice, since slices are not comparable.
type compositeIdentityKey struct {
	parent, this any
}

// WithIdentityKey is a helper function that allows you to layer identity keys.
func WithIdentityKey[T comparable](ctx context.Context, key T) context.Context {
	parent := IdentityFromContext(ctx)
	var genericKey any = key
	if parent != nil {
		genericKey = compositeIdentityKey{
			parent: IdentityFromContext(ctx),
			this:   key,
		}
	}
	return context.WithValue(
		ctx,
		ContextKey,
		genericKey,
	)
}

func IdentityFromContext(ctx context.Context) any {
	return ctx.Value(ContextKey)
}
