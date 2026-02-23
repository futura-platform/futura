package flowhooks_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/flowhooks"
	"github.com/stretchr/testify/assert"
)

func TestWithOnExecutionEnd(t *testing.T) {
	t.Run("WithOnExecutionEnd should be called when the flow execution ends successfully", func(t *testing.T) {
		callCount := 0
		var gotErr error

		r, err := futura.NewFlow[*any, string]().Execute(
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

		r, err := futura.NewFlow[*any, string]().Execute(
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
			r, err := futura.NewFlow[*any, string]().Execute(
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
			r, err := futura.NewFlow[*any, string]().Execute(
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

		_, err := futura.NewFlow[*any, string]().Execute(
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
