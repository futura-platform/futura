package flog

import (
	"io"
	"log/slog"
	"math"
	"os"
	"testing"
)

var (
	noopLogger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		// filter out all logs
		Level: slog.Level(math.MaxInt),
	}))
)

func defaultLogger() *slog.Logger {
	if testing.Testing() && testing.Verbose() {
		return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
	}
	return noopLogger
}
