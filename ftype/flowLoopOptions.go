package ftype

import (
	"context"
	"log/slog"

	flog_internal "github.com/futura-platform/futura/internal/flog"
)

type FlowLoopHooks struct {
	OnError []func(err error) (continueExecution bool)
}

type FlowLoopOptions struct {
	Hooks           FlowLoopHooks
	ContextWrappers []func(context.Context) context.Context
}

type FlowLoopOption func(*FlowLoopOptions)

func WithLogger(logger *slog.Logger) FlowLoopOption {
	return func(opts *FlowLoopOptions) {
		opts.ContextWrappers = append(opts.ContextWrappers, func(ctx context.Context) context.Context {
			return flog_internal.WithLogger(ctx, logger)
		})
	}
}

func WithOnError(onError func(err error) (continueExecution bool)) FlowLoopOption {
	return func(opts *FlowLoopOptions) {
		opts.Hooks.OnError = append(opts.Hooks.OnError, onError)
	}
}
