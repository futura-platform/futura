package moment

import (
	"context"
	"errors"
	"fmt"

	"github.com/futura-platform/futura/ftype/seal"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
)

// Identity is a unique identifier for a moment.
// It is used to identify the specific point in time a moment occurs in the flow.
// The callpath identifies where in the code the moment is defined, and the key identifies the "instance" of the moment.
//
// This is useful for moments produced by loops. This is a similar concept to React's "key" prop.
// "key"s must be comparable, this is enforced in the WithIdentityKey function.
//
// "key" is assigned through context, so that api consumers can have the ability to encapsulate logic that doesn't need to know about the key.
// (like a helper function that could be called inside or out of a loop)
type Identity struct {
	callpath seal.Sealed[Callpath]
	key      string
}

func (i Identity) Callpath() seal.Sealed[Callpath] {
	return i.callpath
}

func NewIdentity(ctx context.Context, callpath Callpath) Identity {
	key, _ := IdentityFromContext(ctx)
	return Identity{
		callpath: seal.Seal(callpath),
		key:      key,
	}
}

func (i Identity) String() string {
	return fmt.Sprintf("key:%v callpath:%s", i.key, i.callpath.V())
}

type contextKey string

const ContextKey contextKey = "futura_moment_identity_key"

// WithIdentityKey is a helper function that allows you to layer identity keys.
func WithIdentityKey(ctx context.Context, key string) context.Context {
	parent, ok := IdentityFromContext(ctx)
	if ok {
		parent += "-"
	}
	return context.WithValue(
		ctx,
		ContextKey,
		// we need a way to stack identity keys, so that WithIdentityKey can be called multiple times to layer keys.
		// we need the structure that stores this stack to be comparable.
		// and we need to not have any interfaces that mess with encoding.
		// This means we must use a string.
		parent+key,
	)
}

func IdentityFromContext(ctx context.Context) (string, bool) {
	value, ok := ctx.Value(ContextKey).(string)
	return value, ok
}

var ErrNoMomentBeingEvaluated = errors.New("no moment is being evaluated")

type currentIdentityKey struct{}

// CurrentIdentity returns the identity of the moment function that is currently executing.
// It panics if called outside of a moment fn.
func CurrentIdentity(ctx context.Context) Identity {
	identity, ok := ctx.Value(currentIdentityKey{}).(Identity)
	if !ok {
		panic(ftrerrors.InconsistentStateError(ErrNoMomentBeingEvaluated))
	}
	return identity
}
