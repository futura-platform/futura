package futura

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFlowSequence(t *testing.T) {
	t.Run("Basic flow", func(t *testing.T) {
		ctx := context.Background()
		rval, err := ExecuteFlow(ctx, nil, func(ctx context.Context) (string, error) {
			return "test", nil
		})
		assert.NoError(t, err)
		assert.Equal(t, "test", rval)
	})

	t.Run("Regular error handling", func(t *testing.T) {
		ctx := context.Background()
		testErr := errors.New("test error")
		onErrCallCount := 0
		opts := &FlowSequenceOptions{
			OnError: func(err error) bool {
				onErrCallCount++
				assert.Equal(t, testErr, err)
				return onErrCallCount < 2
			},
		}
		fnCallCount := 0
		rval, err := ExecuteFlow(ctx, opts, func(ctx context.Context) (string, error) {
			fnCallCount++
			return "", testErr
		})
		assert.Equal(t, 2, onErrCallCount)
		assert.Equal(t, 2, fnCallCount)
		assert.Equal(t, "", rval)
		assert.Error(t, err)
		assert.Equal(t, testErr, err)
	})

	t.Run("Context error handling", func(t *testing.T) {
		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)
		_, err := ExecuteFlow(ctx, nil, func(ctx context.Context) (string, error) {
			cancel()
			return "", errors.New("unrelated forever error")
		})
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("End to end flow with steps", func(t *testing.T) {
		ctx := context.Background()

		fn1Calls := 0
		failsTwice := func(ctx context.Context) (string, error) {
			fn1Calls++
			if fn1Calls <= 2 {
				return "", errors.New("test error")
			}
			return "fn1", nil
		}

		fn2 := func(ctx context.Context) (string, error) {
			return "fn2", nil
		}

		errCount := 0
		rval, err := ExecuteFlow(ctx, &FlowSequenceOptions{
			OnError: func(err error) bool {
				assert.Equal(t, "test error", err.Error())
				errCount++
				return true
			},
		}, func(ctx context.Context) (string, error) {
			r1, err := Step(ctx, nil, failsTwice)
			if err != nil {
				return "", err
			}

			r2, err := Step(ctx, nil, fn2)
			if err != nil {
				return "", err
			}
			return r1 + r2, nil
		})
		assert.NoError(t, err)
		assert.Equal(t, "fn1fn2", rval)
		assert.Equal(t, 2, errCount)
	})
}
