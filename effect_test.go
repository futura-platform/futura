package futura_test

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"testing"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/flow/moment"
	"github.com/futura-platform/futura/internal/step"
	"github.com/stretchr/testify/assert"
)

func myNamedEffectFn(ctx context.Context, _ struct{}) error {
	return errors.New("effect error")
}

func TestEffect(t *testing.T) {
	t.Run("Effect executes the function and returns nil on success", func(t *testing.T) {
		called := false
		_, err := futura.Flow(t.Context(), func(b futura.FlowBuilder, _ struct{}) (struct{}, error) {
			return struct{}{}, futura.Effect(b, func(ctx context.Context, _ struct{}) error {
				called = true
				return nil
			}, struct{}{})
		}, struct{}{})

		assert.NoError(t, err)
		assert.True(t, called, "effect function was not called")
	})

	t.Run("Effect propagates errors from the function", func(t *testing.T) {
		expectedErr := errors.New("effect error")
		_, err := futura.Flow(t.Context(), func(b futura.FlowBuilder, _ struct{}) (struct{}, error) {
			err := futura.Effect(b, func(ctx context.Context, _ struct{}) error {
				return expectedErr
			}, struct{}{})
			assert.ErrorIs(t, err, expectedErr)
			return struct{}{}, nil
		}, struct{}{})

		assert.NoError(t, err)
	})

	t.Run("Effect uses compile-time label from the original function, not the anonymous wrapper", func(t *testing.T) {
		label := moment.CompileTimeLabel(runtime.FuncForPC(reflect.ValueOf(myNamedEffectFn).Pointer()))
		_, err := futura.Flow(t.Context(), func(b futura.FlowBuilder, _ struct{}) (struct{}, error) {
			err := futura.Effect(b, myNamedEffectFn, struct{}{})
			assert.ErrorIs(t, err, step.ErrEvalFailed)
			assert.ErrorContains(t, err, label)
			return struct{}{}, nil
		}, struct{}{})
		assert.NoError(t, err)
	})

	t.Run("Effect uses user-provided label when specified", func(t *testing.T) {
		label := "testLabel"
		_, err := futura.Flow(t.Context(), func(b futura.FlowBuilder, _ struct{}) (struct{}, error) {
			err := futura.Effect(b, myNamedEffectFn, struct{}{}, ftype.WithLabel(label))
			assert.ErrorIs(t, err, step.ErrEvalFailed)
			assert.ErrorContains(t, err, label)
			return struct{}{}, nil
		}, struct{}{})
		assert.NoError(t, err)
	})
}
