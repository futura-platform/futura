package flog

import (
	"context"
	"log/slog"

	flog_internal "github.com/futura-platform/futura/internal/flog"
)

func FromContext(ctx context.Context) *slog.Logger {
	logger, ok := ctx.Value(flog_internal.ContextKey).(*slog.Logger)
	if !ok {
		return defaultLogger()
	}
	return logger
}
