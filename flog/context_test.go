package flog_test

import (
	"io"
	"log/slog"
	"testing"

	"github.com/futura-platform/futura/flog"
	flog_internal "github.com/futura-platform/futura/internal/flog"
	"github.com/stretchr/testify/assert"
)

func TestFromContext(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := flog_internal.WithLogger(t.Context(), logger)
	assert.Equal(t, logger, flog.FromContext(ctx))
}
