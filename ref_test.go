package futura_test

import (
	"context"
	"errors"
	"testing"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/internal/utils/testutil"
	"github.com/stretchr/testify/assert"
)

func TestRef(t *testing.T) {
	t.Run("no initial value implies the default to be the type's zero value", func(t *testing.T) {
		r, err := futura.NewFlow[*struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ *struct{}) (int, error) {
			ref := futura.Ref[int](b)
			return *ref, nil
		}, &struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 0, r)
	})
	t.Run("an initial value can be provided", func(t *testing.T) {
		r, err := futura.NewFlow[*struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ *struct{}) (int, error) {
			return *futura.Ref(b, 1), nil
		}, &struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 1, r)
	})
	t.Run("multiple initial values will cause a panic", func(t *testing.T) {
		assert.Panics(t, func() {
			futura.Ref(futura.FlowBuilder{}, 1, 2)
		})
	})
	t.Run("changes persist across replays", func(t *testing.T) {
		callCount := 0
		failsTwice := func(ctx context.Context, _ *struct{}) (string, error) {
			callCount++
			if callCount <= 2 {
				return "", errors.New("expected error")
			}
			return "success", nil
		}
		actualState := 0
		r, err := futura.NewFlow[*struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ *struct{}) (int, error) {
			state := futura.Ref[int](b)
			assert.Equal(t, actualState, *state)
			actualState++
			*state++

			_, err := futura.Step(b, failsTwice, &struct{}{})
			if err != nil {
				return 0, err
			}
			return *state, nil
		}, &struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 3, callCount)
		assert.Equal(t, 3, r)
	})
	t.Run("a change in the initial value will cause the state to be re-initialized to that new value", func(t *testing.T) {
		srcInitialValue := 0
		r, err := futura.NewFlow[*struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ *struct{}) (int, error) {
			state := futura.Ref(b, srcInitialValue)
			assert.Equal(t, srcInitialValue, *state)
			srcInitialValue++
			if srcInitialValue < 2 {
				return 0, errors.New("expected error")
			}
			return *state, nil
		}, &struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 2, srcInitialValue)
		assert.Equal(t, 1, r)
	})
	t.Run("does not panic on context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		refDidntPanic := false
		r, err := futura.NewFlow[*struct{}, int]().Execute(ctx, func(b futura.FlowBuilder, _ *struct{}) (int, error) {
			cancel()
			ref := futura.Ref(b, 1)
			refDidntPanic = true
			return *ref, nil
		}, &struct{}{})
		assert.ErrorIs(t, err, context.Canceled)
		assert.True(t, refDidntPanic)
		assert.Equal(t, 0, r) // should be the zero value of the type
	})
	t.Run("panics if the evaluation fails", func(t *testing.T) {
		var expectedErr = errors.New("expected error")
		_, err := futura.NewFlow[*struct{}, int]().Execute(
			testutil.WithInjectedError(
				t.Context(),
				testutil.InjectedErrorLevelEvaluate,
				expectedErr,
			),
			func(b futura.FlowBuilder, _ *struct{}) (int, error) {
				futura.Ref(b, 1)
				return 0, nil
			},
			&struct{}{},
		)
		assert.ErrorIs(t, err, futura.ErrFlowPanic)
		assert.ErrorIs(t, err, expectedErr)
	})
}
