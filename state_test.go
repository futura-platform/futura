package futura_test

import (
	"context"
	"errors"
	"testing"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/stretchr/testify/assert"
)

func TestState(t *testing.T) {
	t.Run("no initial value implies the default to be the type's zero value", func(t *testing.T) {
		r, err := futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			state := futura.State[int](b)
			return state.V(), nil
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 0, r)
	})
	t.Run("an initial value can be provided", func(t *testing.T) {
		r, err := futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			state := futura.State(b, 1)
			return state.V(), nil
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 1, r)
	})
	t.Run("multiple initial values will cause a panic", func(t *testing.T) {
		assert.Panics(t, func() {
			futura.State(futura.FlowBuilder{}, 1, 2)
		})
	})
	t.Run("setState does not trigger a replay if the new value is the same as the current value", func(t *testing.T) {
		replays := 0
		futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			replays++
			state := futura.State(b, 1)
			state.Set(1)
			return state.V(), nil
		}, struct{}{})
		assert.Equal(t, 1, replays)
	})
	t.Run("setState updates the state and immediately triggers a replay for a new value", func(t *testing.T) {
		r, err := futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			state := futura.State(b, 1)
			state.Set(2)
			return state.V(), nil
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 2, r)
	})
	t.Run("state is restored after execution is restarted from the same container", func(t *testing.T) {
		flowFn := func(b futura.FlowBuilder, _ struct{}) (int, error) {
			state := futura.State(b, 1)
			if state.V() == 1 {
				state.Set(2)
			}
			return state.V(), nil
		}

		originalContainer := executiontype.NewInMemoryContainer()

		r, err := futura.NewFlowFromContainer[struct{}, int](originalContainer).Execute(t.Context(), flowFn, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 2, r)

		// Simulate handing execution state to another machine/process by creating
		// a new flow instance over the persisted execution container.
		r, err = futura.NewFlowFromContainer[struct{}, int](originalContainer).Execute(t.Context(), flowFn, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 2, r)
	})
	t.Run("state values can be used as branch conditions", func(t *testing.T) {
		fn1Calls := 0
		fn1 := func(_ context.Context, _ struct{}) (int, error) {
			fn1Calls++
			return fn1Calls, nil
		}
		fn2Calls := 0
		failsTwice := func(_ context.Context, _ struct{}) (int, error) {
			fn2Calls++
			if fn2Calls <= 2 {
				return 0, errors.New("expected error")
			}
			return fn2Calls, nil
		}
		r, err := futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			state := futura.State(b, 0)

			var r1, r2 int
			var err error
			if state.V() != 1 {
				r1, err = fn1(b, struct{}{})
				if err != nil {
					return 0, err
				}
			}
			r2, err = failsTwice(b, struct{}{})
			if err != nil {
				state.Set(state.V() + 1)
				return 0, err
			}
			return r1 + r2, nil
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 2, fn1Calls)
		assert.Equal(t, 3, fn2Calls)
		assert.Equal(t, 5, r)
	})
	t.Run("the context can be cancelled before a states initial value is seeded", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		r, err := futura.NewFlow[struct{}, int]().Execute(ctx, func(b futura.FlowBuilder, _ struct{}) (int, error) {
			cancel()
			state := futura.State(b, 1)
			return state.V(), nil
		}, struct{}{})
		assert.ErrorIs(t, err, context.Canceled)
		assert.NotErrorIs(t, err, futura.ErrFlowPanic)
		assert.Equal(t, 0, r)
	})
}
