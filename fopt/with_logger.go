package fopt

import (
	"context"
	"log/slog"

	"github.com/futura-platform/futura/flog"
	"github.com/futura-platform/futura/ftype"
)

func WithLogger(logger *slog.Logger) ftype.FlowLoopOption {
	return func(ctx context.Context) context.Context {
		return flog.WithLogger(ctx, logger)
	}
}
