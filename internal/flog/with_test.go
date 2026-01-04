package flog_internal

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithLogger(t *testing.T) {
	t.Run("returns a new context with the logger @ the key", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		ctx := WithLogger(t.Context(), logger)
		assert.Equal(t, logger, ctx.Value(ContextKey))
	})
}
