package flowhooks_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flowhooks"
	"github.com/futura-platform/futura/internal/utils/containertest"
	"github.com/stretchr/testify/assert"
)

func TestWithOnExecutionEnd(t *testing.T) {
	t.Run("WithOnExecutionEnd should be called when the flow execution ends successfully", func(t *testing.T) {
		callCount := 0
		var gotErr error

		r, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(
			t.Context(),
			func(b futura.FlowBuilder, _ *any) (string, error) {
				return "success", nil
			},
			nil,
			flowhooks.WithOnExecutionEnd(func(ctx context.Context, err error) error {
				callCount++
				gotErr = err
				return nil
			}),
		)
		assert.NoError(t, err)
		assert.Equal(t, "success", r)
		assert.Equal(t, 1, callCount)
		assert.NoError(t, gotErr)
	})

	t.Run("WithOnExecutionEnd should be called when the flow execution ends with an error", func(t *testing.T) {
		callCount := 0
		var gotErr error

		r, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(
			t.Context(),
			func(b futura.FlowBuilder, _ *any) (string, error) {
				return "cancelled", ftype.ErrCancelFlow
			},
			nil,
			flowhooks.WithOnExecutionEnd(func(ctx context.Context, err error) error {
				callCount++
				gotErr = err
				return nil
			}),
		)
		assert.ErrorIs(t, err, ftype.ErrCancelFlow)
		assert.Equal(t, "cancelled", r)
		assert.Equal(t, 1, callCount)
		assert.ErrorIs(t, gotErr, ftype.ErrCancelFlow)
	})

	t.Run("WithOnExecutionEnd's error should be joined with the flow error", func(t *testing.T) {
		onEndError := errors.New("onEnd error")
		instrumentForTestOpt := flowhooks.WithOnExecutionEnd(func(ctx context.Context, err error) error {
			return onEndError
		})
		t.Run("when the flow error is nil", func(t *testing.T) {
			flowErr := errors.New("flow error")
			r, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(
				t.Context(),
				func(b futura.FlowBuilder, _ *any) (string, error) {
					return "expected", fmt.Errorf("%w: %w", ftype.ErrCancelFlow, flowErr)
				},
				nil,
				instrumentForTestOpt,
			)
			assert.ErrorIs(t, err, flowErr)
			assert.ErrorIs(t, err, onEndError)
			assert.Equal(t, "expected", r)
		})
		t.Run("when the flow error is not nil", func(t *testing.T) {
			r, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(
				t.Context(),
				func(b futura.FlowBuilder, _ *any) (string, error) {
					return "expected", nil
				},
				nil,
				instrumentForTestOpt,
			)
			assert.ErrorIs(t, err, onEndError)
			assert.Equal(t, "expected", r)
		})
	})

	t.Run("a hook sees an execution that ended by panic as a failure, and its error is kept", func(t *testing.T) {
		hookErr := errors.New("hook failed")
		var seen error
		calls := 0
		_, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(
			t.Context(),
			func(b futura.FlowBuilder, _ *any) (string, error) { panic("boom") },
			nil,
			flowhooks.WithOnExecutionEnd(func(ctx context.Context, err error) error {
				calls++
				seen = err
				return hookErr
			}),
		)
		assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
		assert.ErrorIs(t, err, hookErr)
		assert.Equal(t, 1, calls)
		assert.ErrorIs(t, seen, ftrerrors.ErrFlowPanic)
	})

	t.Run("a hook that panics does not stop the others, and is reported", func(t *testing.T) {
		var order []string
		_, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(
			t.Context(),
			func(b futura.FlowBuilder, _ *any) (string, error) { return "success", nil },
			nil,
			flowhooks.WithOnExecutionEnd(func(ctx context.Context, err error) error { order = append(order, "first"); return nil }),
			flowhooks.WithOnExecutionEnd(func(ctx context.Context, err error) error { panic("second hook panics") }),
			flowhooks.WithOnExecutionEnd(func(ctx context.Context, err error) error { order = append(order, "third"); return nil }),
		)
		assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
		assert.ErrorContains(t, err, "second hook panics")
		assert.Equal(t, []string{"third", "first"}, order)
	})

	t.Run("Multiple WithOnExecutionEnd options should be called reverse of their registration order", func(t *testing.T) {
		callOrder := []string{}
		onEnd1 := func(ctx context.Context, err error) error {
			callOrder = append(callOrder, "onEnd1")
			return nil
		}
		onEnd2 := func(ctx context.Context, err error) error {
			callOrder = append(callOrder, "onEnd2")
			return nil
		}

		_, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(
			t.Context(),
			func(b futura.FlowBuilder, _ *any) (string, error) {
				return "success", nil
			},
			nil,
			flowhooks.WithOnExecutionEnd(onEnd1),
			flowhooks.WithOnExecutionEnd(onEnd2),
		)
		assert.NoError(t, err)
		assert.Equal(t, []string{"onEnd2", "onEnd1"}, callOrder)
	})
}

func TestWithOnExecutionEnd_DerivedContextsDoNotShareHooks(t *testing.T) {
	var ran []string
	hook := func(name string) ftype.FlowLoopOption {
		return flowhooks.WithOnExecutionEnd(func(context.Context, error) error { ran = append(ran, name); return nil })
	}
	// enough hooks on the base for its slice to have spare capacity
	base := t.Context()
	for i := range 5 {
		base = hook(fmt.Sprintf("base%d", i))(base)
	}
	a := hook("a")(base)
	b := hook("b")(base)

	assert.NoError(t, flowhooks.RunOnExecutionEnd(a, nil))
	assert.Equal(t, "a", ran[0], "a's own hook runs first")
	ran = nil
	assert.NoError(t, flowhooks.RunOnExecutionEnd(b, nil))
	assert.Equal(t, "b", ran[0])
}
