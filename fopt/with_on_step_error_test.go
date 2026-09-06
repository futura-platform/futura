package fopt_test

import (
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/fopt"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/utils/containertest"
	"github.com/stretchr/testify/assert"
)

func TestWithOnStepError(t *testing.T) {
	t.Run("WithOnStepError should be called when the flow loop encounters an error. Returning false from OnError should stop the flow loop", func(t *testing.T) {
		onErrorCallCount := 0
		onError := func(ctx context.Context, fnLabel string, callstack []runtime.Frame, err error) (continueExecution bool) {
			assert.Equal(t, "test-step", fnLabel)
			assert.NotEmpty(t, callstack)
			onErrorCallCount++
			return onErrorCallCount < 3
		}
		testErr := errors.New("test error")
		r, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
			_, err := futura.Step(b, func(ctx context.Context, args *any) (string, error) {
				return "", testErr
			}, nil, ftype.WithLabel("test-step"))
			if err != nil {
				return "failed", err
			}
			return "success", nil
		}, nil, fopt.WithOnStepError(onError))
		assert.Equal(t, 3, onErrorCallCount)
		assert.ErrorIs(t, err, testErr)
		assert.Equal(t, "failed", r)
	})

	t.Run("Multiple WithOnStepError options should be called reverse of their registration order", func(t *testing.T) {
		callOrder := []string{}
		onError1 := func(ctx context.Context, fnLabel string, callstack []runtime.Frame, err error) (continueExecution bool) {
			assert.Equal(t, "test-step", fnLabel)
			assert.NotEmpty(t, callstack)
			callOrder = append(callOrder, "onError1")
			return true
		}
		onError2 := func(ctx context.Context, fnLabel string, callstack []runtime.Frame, err error) (continueExecution bool) {
			assert.Equal(t, "test-step", fnLabel)
			assert.NotEmpty(t, callstack)
			callOrder = append(callOrder, "onError2")
			return false
		}
		testErr := errors.New("test error")
		r, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
			_, err := futura.Step(b, func(ctx context.Context, args *any) (string, error) {
				return "", testErr
			}, nil, ftype.WithLabel("test-step"))
			if err != nil {
				return "failed", err
			}
			return "success", nil
		}, nil, fopt.WithOnStepError(onError1), fopt.WithOnStepError(onError2))
		assert.Equal(t, []string{"onError2", "onError1"}, callOrder)
		assert.ErrorIs(t, err, testErr)
		assert.Equal(t, "failed", r)
	})

	t.Run("onError does not fire for a restart recovered by a wrapper beneath WithOnStepError", func(t *testing.T) {
		var fired []error
		onError := func(ctx context.Context, fnLabel string, callstack []runtime.Frame, err error) (continueExecution bool) {
			fired = append(fired, err)
			return true
		}
		r, err := futura.NewFlowFromContainer[any, int](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ any) (int, error) {
			s := futura.State(b, 0)
			return futura.Step(b, func(ctx context.Context, _ *struct{}) (int, error) {
				if v := s.V(); v < 3 {
					s.Set(v + 1)
					return 0, ctx.Err()
				}
				return s.V(), nil
			}, nil)
		}, nil, fopt.WithOnStepError(onError), fopt.WithStepWrapper(recoveringStepWrapper))
		assert.NoError(t, err)
		assert.Equal(t, 3, r)
		assert.Empty(t, fired)
	})
}
