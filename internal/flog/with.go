package flog_internal

import (
	"context"
	"log/slog"
)

type contextKey string

const ContextKey contextKey = "futura_slog"

func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, ContextKey, logger)
}
