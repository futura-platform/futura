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

// A moment fn is part of its identity: reaching the same callsite with a different fn is a new moment.
// After a state change legitimately selects the other fn, it must run rather than reuse the first fn's memo.
// Step gets this from the fn passed to it; the helpers wrap the user's fn, so they must identify the moment
// by the user's fn and not by their wrapper.
func TestHelpersIdentifyTheMomentByTheUserFn(t *testing.T) {
	// the flow selects a fn by state, so the second replay legitimately reaches the same callsite with the other fn
	swapAfterFirstReplay := func(t *testing.T, evaluate func(b futura.FlowBuilder, useSecond bool) error) error {
		t.Helper()
		_, err := futura.NewFlowFromContainer[struct{}, struct{}](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (struct{}, error) {
			useSecond := futura.State(b, false)
			if err := evaluate(b, useSecond.V()); err != nil {
				return struct{}{}, err
			}
			if !useSecond.V() {
				useSecond.Set(true)
			}
			return struct{}{}, nil
		}, struct{}{})
		return err
	}

	t.Run("Step", func(t *testing.T) {
		firstCalls, secondCalls := 0, 0
		first := func(ctx context.Context, _ struct{}) (struct{}, error) { firstCalls++; return struct{}{}, nil }
		second := func(ctx context.Context, _ struct{}) (struct{}, error) { secondCalls++; return struct{}{}, nil }
		err := swapAfterFirstReplay(t, func(b futura.FlowBuilder, useSecond bool) error {
			fn := first
			if useSecond {
				fn = second
			}
			_, err := futura.Step(b, fn, struct{}{})
			return err
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, firstCalls)
		assert.Equal(t, 1, secondCalls)
	})

	t.Run("Effect", func(t *testing.T) {
		firstCalls, secondCalls := 0, 0
		first := func(ctx context.Context, _ struct{}) error { firstCalls++; return nil }
		second := func(ctx context.Context, _ struct{}) error { secondCalls++; return nil }
		err := swapAfterFirstReplay(t, func(b futura.FlowBuilder, useSecond bool) error {
			fn := first
			if useSecond {
				fn = second
			}
			return futura.Effect(b, fn, struct{}{})
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, firstCalls)
		assert.Equal(t, 1, secondCalls, "the second fn must run, not reuse the first fn's memo")
	})

	t.Run("Source", func(t *testing.T) {
		firstCalls, secondCalls := 0, 0
		first := func(ctx context.Context) (int, error) { firstCalls++; return 1, nil }
		second := func(ctx context.Context) (int, error) { secondCalls++; return 2, nil }
		err := swapAfterFirstReplay(t, func(b futura.FlowBuilder, useSecond bool) error {
			fn := first
			if useSecond {
				fn = second
			}
			_, err := futura.Source(b, fn)
			return err
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, firstCalls)
		assert.Equal(t, 1, secondCalls, "the second fn must run, not reuse the first fn's memo")
	})

	t.Run("Action", func(t *testing.T) {
		firstCalls, secondCalls := 0, 0
		first := func(ctx context.Context) error { firstCalls++; return nil }
		second := func(ctx context.Context) error { secondCalls++; return nil }
		err := swapAfterFirstReplay(t, func(b futura.FlowBuilder, useSecond bool) error {
			fn := first
			if useSecond {
				fn = second
			}
			return futura.Action(b, fn)
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, firstCalls)
		assert.Equal(t, 1, secondCalls, "the second fn must run, not reuse the first fn's memo")
	})
}
