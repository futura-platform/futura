package fopt

import (
	"context"
	"errors"

	"github.com/futura-platform/futura/ftype"
)

// WithOnExecutionEnd registers a callback that will be invoked once when a top-level
// flow execution ends (success or error).
//
// If multiple callbacks are registered, they are invoked in reverse order of
// registration (LIFO), mirroring Go's defer behavior.
//
// The callback's returned error (if any) will be joined onto the final flow error
// by the top-level flow runner.
func WithOnExecutionEnd(onEnd func(ctx context.Context, err error) error) ftype.FlowLoopOption {
	return func(ctx context.Context) context.Context {
		if onEnd == nil {
			panic("WithOnExecutionEnd callback cannot be nil")
		}
		existing, _ := ctx.Value(executionEndHooksKey).([]executionEndHook)
		hooks := append(existing, executionEndHook(onEnd))
		return context.WithValue(ctx, executionEndHooksKey, hooks)
	}
}

type executionEndHook func(ctx context.Context, err error) error

type executionEndHooksCtxKey struct{}

var executionEndHooksKey executionEndHooksCtxKey

// RunOnExecutionEnd executes callbacks registered by WithOnExecutionEnd, returning
// any errors they produced (joined).
//
// Callbacks are invoked in reverse order of registration (LIFO).
func RunOnExecutionEnd(ctx context.Context, executionErr error) error {
	hooks, _ := ctx.Value(executionEndHooksKey).([]executionEndHook)
	var hookErr error
	for i := len(hooks) - 1; i >= 0; i-- {
		if err := hooks[i](ctx, executionErr); err != nil {
			hookErr = errors.Join(hookErr, err)
		}
	}
	return hookErr
}
