package ftype

import (
	"context"
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
