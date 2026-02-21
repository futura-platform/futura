package flog

import (
	"context"
	"log/slog"
)

type contextKeyType string

const contextKey contextKeyType = "futura_slog"

func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, contextKey, logger)
}

func FromContext(ctx context.Context) *slog.Logger {
	logger, ok := ctx.Value(contextKey).(*slog.Logger)
	if !ok {
		return defaultLogger()
	}
	return logger
}
