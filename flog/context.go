package flog

import (
	"context"
	"io"
	"log/slog"
	"math"
	"os"
	"testing"

	flog_internal "github.com/futura-platform/futura/internal/flog"
)

var noopLogger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
	// filter out all logs
	Level: slog.Level(math.MaxInt),
}))

func FromContext(ctx context.Context) *slog.Logger {
	logger, ok := ctx.Value(flog_internal.ContextKey).(*slog.Logger)
	if !ok {
		if testing.Testing() && testing.Verbose() {
			return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			}))
		}
		return noopLogger
	}
	return logger
}
