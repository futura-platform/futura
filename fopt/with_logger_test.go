package fopt_test

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/fopt"
	"github.com/futura-platform/futura/internal/utils/containertest"
	"github.com/stretchr/testify/assert"
)

func TestWithLogger(t *testing.T) {
	t.Run("WithLogger should make the flow use the provided logger", func(t *testing.T) {
		logBuf := bytes.NewBuffer(nil)
		logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
		r, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
			err := futura.Effect(b, func(ctx context.Context, args *any) error { return nil }, nil)
			if err != nil {
				return "failed", err
			}
			return "success", nil
		}, nil, fopt.WithLogger(logger))
		assert.NoError(t, err)
		assert.Equal(t, "success", r)
		assert.Positive(t, logBuf.Len())
	})
}
