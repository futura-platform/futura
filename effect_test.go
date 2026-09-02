package futura_test

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"testing"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/step"
	"github.com/futura-platform/futura/internal/utils/containertest"
	"github.com/futura-platform/futura/moment"
	"github.com/stretchr/testify/assert"
)

func myNamedEffectFn(ctx context.Context, _ struct{}) error {
	return errors.New("effect error")
}

func myNamedSourceFn(ctx context.Context) (struct{}, error) {
	return struct{}{}, errors.New("source error")
}

func myNamedActionFn(ctx context.Context) error {
	return errors.New("action error")
}

func TestEffect(t *testing.T) {
	t.Run("Effect executes the function and returns nil on success", func(t *testing.T) {
		called := false
		_, err := futura.NewFlowFromContainer[struct{}, struct{}](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (struct{}, error) {
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
		_, err := futura.NewFlowFromContainer[struct{}, struct{}](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (struct{}, error) {
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
		_, err := futura.NewFlowFromContainer[struct{}, struct{}](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (struct{}, error) {
			err := futura.Effect(b, myNamedEffectFn, struct{}{})
			assert.ErrorIs(t, err, step.ErrEvalFailed)
			assert.ErrorContains(t, err, label)
			return struct{}{}, nil
		}, struct{}{})
		assert.NoError(t, err)
	})

	t.Run("Effect uses user-provided label when specified", func(t *testing.T) {
		label := "testLabel"
		_, err := futura.NewFlowFromContainer[struct{}, struct{}](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (struct{}, error) {
			err := futura.Effect(b, myNamedEffectFn, struct{}{}, ftype.WithLabel(label))
			assert.ErrorIs(t, err, step.ErrEvalFailed)
			assert.ErrorContains(t, err, label)
			return struct{}{}, nil
		}, struct{}{})
		assert.NoError(t, err)
	})
}

func TestSource(t *testing.T) {
	t.Run("Source executes the function and returns output on success", func(t *testing.T) {
		called := false
		output, err := futura.NewFlowFromContainer[struct{}, string](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (string, error) {
			return futura.Source(b, func(ctx context.Context) (string, error) {
				called = true
				return "source output", nil
			})
		}, struct{}{})

		assert.NoError(t, err)
		assert.Equal(t, "source output", output)
		assert.True(t, called, "source function was not called")
	})

	t.Run("Source propagates errors from the function", func(t *testing.T) {
		expectedErr := errors.New("source error")
		_, err := futura.NewFlowFromContainer[struct{}, struct{}](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (struct{}, error) {
			_, err := futura.Source(b, func(ctx context.Context) (struct{}, error) {
				return struct{}{}, expectedErr
			})
			assert.ErrorIs(t, err, expectedErr)
			return struct{}{}, nil
		}, struct{}{})

		assert.NoError(t, err)
	})

	t.Run("Source uses compile-time label from the original function, not the anonymous wrapper", func(t *testing.T) {
		label := moment.CompileTimeLabel(runtime.FuncForPC(reflect.ValueOf(myNamedSourceFn).Pointer()))
		_, err := futura.NewFlowFromContainer[struct{}, struct{}](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (struct{}, error) {
			_, err := futura.Source(b, myNamedSourceFn)
			assert.ErrorIs(t, err, step.ErrEvalFailed)
			assert.ErrorContains(t, err, label)
			return struct{}{}, nil
		}, struct{}{})
		assert.NoError(t, err)
	})

	t.Run("Source uses user-provided label when specified", func(t *testing.T) {
		label := "testLabel"
		_, err := futura.NewFlowFromContainer[struct{}, struct{}](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (struct{}, error) {
			_, err := futura.Source(b, myNamedSourceFn, ftype.WithLabel(label))
			assert.ErrorIs(t, err, step.ErrEvalFailed)
			assert.ErrorContains(t, err, label)
			return struct{}{}, nil
		}, struct{}{})
		assert.NoError(t, err)
	})
}

func TestAction(t *testing.T) {
	t.Run("Action executes the function and returns nil on success", func(t *testing.T) {
		calls := 0
		_, err := futura.NewFlowFromContainer[struct{}, struct{}](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (struct{}, error) {
			return struct{}{}, futura.Action(b, func(ctx context.Context) error {
				calls++
				return nil
			})
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 1, calls)
	})

	t.Run("Action uses compile-time label from the original function, not the anonymous wrapper", func(t *testing.T) {
		label := moment.CompileTimeLabel(runtime.FuncForPC(reflect.ValueOf(myNamedActionFn).Pointer()))
		_, err := futura.NewFlowFromContainer[struct{}, struct{}](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (struct{}, error) {
			err := futura.Action(b, myNamedActionFn)
			assert.ErrorIs(t, err, step.ErrEvalFailed)
			assert.ErrorContains(t, err, label)
			return struct{}{}, nil
		}, struct{}{})
		assert.NoError(t, err)
	})

	t.Run("Action uses user-provided label when specified", func(t *testing.T) {
		label := "testLabel"
		_, err := futura.NewFlowFromContainer[struct{}, struct{}](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (struct{}, error) {
			err := futura.Action(b, myNamedActionFn, ftype.WithLabel(label))
			assert.ErrorIs(t, err, step.ErrEvalFailed)
			assert.ErrorContains(t, err, label)
			return struct{}{}, nil
		}, struct{}{})
		assert.NoError(t, err)
	})
}
