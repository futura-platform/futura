package ftype

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"

	flog_internal "github.com/futura-platform/futura/internal/flog"
	stepwrapper "github.com/futura-platform/futura/internal/step/wrapper"
)

type FlowLoopOption func(context.Context) context.Context

func WithLogger(logger *slog.Logger) FlowLoopOption {
	return func(ctx context.Context) context.Context {
		return flog_internal.WithLogger(ctx, logger)
	}
}

func WithStepWrapper(wrapper stepwrapper.StepWrapper) FlowLoopOption {
	return func(ctx context.Context) context.Context {
		return stepwrapper.With(ctx, wrapper)
	}
}

func WithOnStepError(onError func(err error) (continueExecution bool)) FlowLoopOption {
	return WithStepWrapper(func(ctx context.Context, args any, callstack []runtime.Frame, call func() (output any, err error)) (errOverride error) {
		_, err := call()
		if err != nil && !onError(err) {
			return fmt.Errorf("%w: %w", ErrCancelFlow, err)
		}
		return nil
	})
}

// WithOnExecutionEnd registers a callback that will be invoked once when a top-level
// flow execution ends (success or error).
//
// If multiple callbacks are registered, they are invoked in reverse order of
// registration (LIFO), mirroring Go's defer behavior.
//
// The callback's returned error (if any) will be joined onto the final flow error
// by the top-level flow runner.
func WithOnExecutionEnd(onEnd func(ctx context.Context, err error) error) FlowLoopOption {
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
