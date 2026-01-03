package testutil

import (
	"context"
	"testing"
)

type injectedErrorLevel int

const (
	InjectedErrorLevelEvaluate injectedErrorLevel = iota
)

type injectedErrorKey struct {
	level injectedErrorLevel
}

// WithInjectedError returns a context that carries an injected error for testing.
// Use InjectedError to retrieve it.
func WithInjectedError(ctx context.Context, level injectedErrorLevel, err error) context.Context {
	if !testing.Testing() {
		panic("InjectedError is only available in testing")
	}
	return context.WithValue(ctx, injectedErrorKey{level}, err)
}

// InjectedError retrieves an injected error from the context, if any.
// Returns nil if no error was injected.
func InjectedError(ctx context.Context, level injectedErrorLevel) error {
	if !testing.Testing() {
		return nil
	}

	if err, ok := ctx.Value(injectedErrorKey{level}).(error); ok {
		return err
	}
	return nil
}
