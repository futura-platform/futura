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
	t.Run("every runtime log line goes through the flow's logger", func(t *testing.T) {
		flowLog := bytes.NewBuffer(nil)
		globalLog := bytes.NewBuffer(nil)
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(globalLog, &slog.HandlerOptions{Level: slog.LevelDebug})))
		defer slog.SetDefault(previous)

		_, err := futura.NewFlowFromContainer[*any, int](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (int, error) {
			s := futura.State(b, 0)
			if s.V() == 0 {
				s.Set(1) // restarts the replay, which the runtime logs
			}
			return s.V(), nil
		}, nil, fopt.WithLogger(slog.New(slog.NewTextHandler(flowLog, &slog.HandlerOptions{Level: slog.LevelDebug}))))
		assert.NoError(t, err)
		assert.Contains(t, flowLog.String(), "restarting replay")
		assert.Empty(t, globalLog.String(), "the runtime logged past the flow's logger")
	})
}
