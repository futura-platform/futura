package flog

import (
	"io"
	"log/slog"
	"testing"

	flog_internal "github.com/futura-platform/futura/internal/flog"
	"github.com/stretchr/testify/assert"
)

func TestFromContext(t *testing.T) {
	t.Run("returns the logger from the context", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		ctx := flog_internal.WithLogger(t.Context(), logger)
		assert.Equal(t, logger, FromContext(ctx))
	})
	t.Run("returns the default logger if no logger is in the context", func(t *testing.T) {
		assert.Equal(t, defaultLogger(), FromContext(t.Context()))
	})
}
