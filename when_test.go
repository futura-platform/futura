package futura_test

import (
	"context"
	"errors"
	"testing"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/internal/utils/containertest"
	"github.com/stretchr/testify/assert"
)

func TestWhen(t *testing.T) {
	t.Run("does not run the body when the condition is false", func(t *testing.T) {
		bodyCalls := 0
		_, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			return 0, futura.When(b, false, func(b futura.FlowBuilder) error {
				bodyCalls++
				return nil
			})
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 0, bodyCalls)
	})
	t.Run("runs the body when the condition is true", func(t *testing.T) {
		stepCalls := 0
		_, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			return 0, futura.When(b, true, func(b futura.FlowBuilder) error {
				return futura.Action(b, func(ctx context.Context) error {
					stepCalls++
					return nil
				})
			})
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 1, stepCalls)
	})
	t.Run("errors from the body are returned", func(t *testing.T) {
		expectedErr := errors.New("expected error")
		_, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			return 0, futura.When(b, true, func(b futura.FlowBuilder) error {
				return expectedErr
			})
		}, struct{}{})
		assert.ErrorIs(t, err, expectedErr)
	})
	t.Run("steps inside the branch are memoized across replays while it stays open", func(t *testing.T) {
		stepCalls := 0
		retries := 0
		_, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			if err := futura.When(b, true, func(b futura.FlowBuilder) error {
				return futura.Action(b, func(ctx context.Context) error {
					stepCalls++
					return nil
				})
			}); err != nil {
				return 0, err
			}
			return 0, futura.Action(b, func(ctx context.Context) error {
				retries++
				if retries < 3 {
					return errors.New("retry")
				}
				return nil
			})
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 3, retries)
		assert.Equal(t, 1, stepCalls)
	})
	t.Run("steps inside the branch run fresh when it is reopened", func(t *testing.T) {
		stepCalls := 0
		_, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			// open -> closed -> open, driven by a state so the transitions replay
			phase := futura.State(b, 0)
			if err := futura.When(b, phase.V() != 1, func(b futura.FlowBuilder) error {
				return futura.Action(b, func(ctx context.Context) error {
					stepCalls++
					return nil
				})
			}); err != nil {
				return 0, err
			}
			if phase.V() < 2 {
				phase.Set(phase.V() + 1)
			}
			return 0, nil
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 2, stepCalls)
	})
	t.Run("steps outside the branch are unaffected by it reopening", func(t *testing.T) {
		outsideCalls := 0
		_, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			phase := futura.State(b, 0)
			if err := futura.When(b, phase.V() != 1, func(b futura.FlowBuilder) error {
				return nil
			}); err != nil {
				return 0, err
			}
			if err := futura.Action(b, func(ctx context.Context) error {
				outsideCalls++
				return nil
			}); err != nil {
				return 0, err
			}
			if phase.V() < 2 {
				phase.Set(phase.V() + 1)
			}
			return 0, nil
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 1, outsideCalls)
	})
	t.Run("setting a condition and setting it back before the next replay is not a transition", func(t *testing.T) {
		stepCalls := 0
		_, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			open := futura.State(b, true)
			flipped := futura.State(b, false)
			if err := futura.When(b, open.V(), func(b futura.FlowBuilder) error {
				return futura.Action(b, func(ctx context.Context) error {
					stepCalls++
					return nil
				})
			}); err != nil {
				return 0, err
			}
			if !flipped.V() {
				flipped.Set(true)
				// both writes land before the replay restarts, so the next replay
				// observes the condition as still open
				open.Set(false)
				open.Set(true)
			}
			return 0, nil
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 1, stepCalls)
	})
	t.Run("nested branches reopen independently", func(t *testing.T) {
		innerCalls := 0
		_, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			phase := futura.State(b, 0)
			if err := futura.When(b, true, func(b futura.FlowBuilder) error {
				return futura.When(b, phase.V() != 1, func(b futura.FlowBuilder) error {
					return futura.Action(b, func(ctx context.Context) error {
						innerCalls++
						return nil
					})
				})
			}); err != nil {
				return 0, err
			}
			if phase.V() < 2 {
				phase.Set(phase.V() + 1)
			}
			return 0, nil
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 2, innerCalls)
	})
}
