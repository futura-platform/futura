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
//
// "key" is assigned through context, so that api consumers can have the ability to encapsulate logic that doesn't need to know about the key.
// (like a helper function that could be called inside or out of a loop)
type Identity struct {
	callpath seal.Sealed[Callpath]
	key      seal.Sealed[[]string]
}

func (i Identity) Callpath() seal.Sealed[Callpath] {
	return i.callpath
}

func NewIdentity(ctx context.Context, callpath Callpath) Identity {
	key, _ := IdentityFromContext(ctx)
	return Identity{
		callpath: seal.Seal(callpath),
		key:      seal.Seal(key),
	}
}

func (i Identity) String() string {
	return fmt.Sprintf("key:%q callpath:%s", i.key.V(), i.callpath.V())
}

type contextKey string

const ContextKey contextKey = "futura_moment_identity_key"

// WithIdentityKey is a helper function that allows you to layer identity keys.
func WithIdentityKey(ctx context.Context, key string) context.Context {
	parent, _ := IdentityFromContext(ctx)
	keys := make([]string, 0, len(parent)+1)
	keys = append(keys, parent...)
	return context.WithValue(ctx, ContextKey, append(keys, key))
}

// IdentityFromContext returns the layered keys on ctx, outermost first.
func IdentityFromContext(ctx context.Context) ([]string, bool) {
	value, ok := ctx.Value(ContextKey).([]string)
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

// IsEvaluating reports whether ctx belongs to a moment function that is currently executing.
func IsEvaluating(ctx context.Context) bool {
	_, ok := ctx.Value(currentIdentityKey{}).(Identity)
	return ok
}
