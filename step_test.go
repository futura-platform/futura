package futura

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInferStepLabel(t *testing.T) {
	label := inferStepLabel(labelToInfer)
	if label != "labelToInfer" {
		t.Errorf("expected labelToInfer, got %s", label)
	}

	label = inferStepLabel(func(ctx context.Context) (string, error) { return "", nil })
	prefix := label[0:4]
	if prefix != "func" {
		t.Errorf("expected func prefix, got %s", prefix)
	}

	suffix := label[4:]
	if suffix != "1" {
		t.Errorf("expected Infer suffix, got %s", suffix)
	}
}

func labelToInfer(ctx context.Context) (*string, error) {
	return nil, nil
}

func TestStep(t *testing.T) {
	t.Run("memoize result", func(t *testing.T) {
		ctx := withFlow(context.Background())
		f := mustGetFlowContext(ctx)

		expectedResult := "expectedResult"
		callCount := 0
		fn := func(ctx context.Context) (string, error) {
			callCount++
			return expectedResult, nil
		}

		result1, err := Step(ctx, nil, fn)
		assert.NoError(t, err)
		assert.Equal(t, expectedResult, result1)
		assert.Equal(t, 1, f.sequenceIndex)
		assert.Equal(t, 1, callCount)

		// simulate going back to the beginning of the flow sequence
		f.sequenceIndex = 0
		result2, err := Step(ctx, nil, fn)
		assert.NoError(t, err)
		assert.Equal(t, result1, result2)
		assert.Equal(t, 1, f.sequenceIndex)
		assert.Equal(t, 1, callCount)
	})

	t.Run("does not memoize error", func(t *testing.T) {
		ctx := withFlow(context.Background())
		f := mustGetFlowContext(ctx)

		expectedError := errors.New("expectedError")
		callCount := 0
		fn := func(ctx context.Context) (string, error) {
			callCount++
			return "", expectedError
		}

		_, err := Step(ctx, nil, fn)
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		assert.Equal(t, 1, f.sequenceIndex)
		assert.Equal(t, 1, callCount)

		// simulate going back to the beginning of the flow sequence
		f.sequenceIndex = 0
		_, err = Step(ctx, nil, fn)
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		assert.Equal(t, 1, f.sequenceIndex)
		assert.Equal(t, 2, callCount)
	})

	t.Run("impure flow detection", func(t *testing.T) {
		ctx := withFlow(context.Background())
		f := mustGetFlowContext(ctx)

		fn1 := func(ctx context.Context) (string, error) {
			return "", nil
		}
		fn2 := func(ctx context.Context) (string, error) {
			return "", nil
		}

		Step(ctx, nil, fn1)
		f.sequenceIndex = 0
		assert.Panics(t, func() {
			Step(ctx, nil, fn2)
		})
		assert.Equal(t, 0, f.sequenceIndex)
	})
}
