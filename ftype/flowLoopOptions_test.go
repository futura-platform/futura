package ftype_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype"
	"github.com/stretchr/testify/assert"
)

func TestWithOnError(t *testing.T) {
	t.Run("WithOnError should be called when the flow loop encounters an error. Returning false from OnError should stop the flow loop", func(t *testing.T) {
		onErrorCallCount := 0
		onError := func(err error) (continueExecution bool) {
			onErrorCallCount++
			return onErrorCallCount < 3
		}
		testErr := errors.New("test error")
		replays := 0
		r, err := futura.Flow(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
			replays++
			_, err := futura.Step(b, func(ctx context.Context, args *any) (string, error) {
				return "", testErr
			}, nil)
			if err != nil {
				return "failed", err
			}
			return "success", nil
		}, nil, ftype.WithOnError(onError))
		assert.Equal(t, 3, onErrorCallCount)
		assert.ErrorIs(t, err, testErr)
		assert.Equal(t, "failed", r)
	})

	t.Run("Multiple OnError hooks should be called in their registration order", func(t *testing.T) {
		var onError1Calls, onError2Calls int
		onError1 := func(err error) (continueExecution bool) {
			onError1Calls++
			return true
		}
		onError2 := func(err error) (continueExecution bool) {
			assert.Equal(t, 1, onError1Calls)
			onError2Calls++
			return false
		}
		testErr := errors.New("test error")
		r, err := futura.Flow(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
			_, err := futura.Step(b, func(ctx context.Context, args *any) (string, error) {
				return "", testErr
			}, nil)
			if err != nil {
				return "failed", err
			}
			return "success", nil
		}, nil, ftype.WithOnError(onError1), ftype.WithOnError(onError2))
		assert.Equal(t, 1, onError1Calls)
		assert.Equal(t, 1, onError2Calls)
		assert.ErrorIs(t, err, testErr)
		assert.Equal(t, "failed", r)
	})
}

func TestWithLogger(t *testing.T) {
	t.Run("WithLogger should make the flow use the provided logger", func(t *testing.T) {
		logBuf := bytes.NewBuffer(nil)
		logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
		r, err := futura.Flow(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
			err := futura.Effect(b, func(ctx context.Context, args *any) error { return nil }, nil)
			if err != nil {
				return "failed", err
			}
			return "success", nil
		}, nil, ftype.WithLogger(logger))
		assert.NoError(t, err)
		assert.Equal(t, "success", r)
		assert.Positive(t, logBuf.Len())
	})
}
