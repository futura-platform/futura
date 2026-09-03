package futura_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/futura-platform/futura/internal/step"
	"github.com/futura-platform/futura/internal/utils/containertest"
	"github.com/stretchr/testify/assert"
)

func TestStep(t *testing.T) {
	t.Run("a step under a context the flow cancelled returns the cancellation as a step error", func(t *testing.T) {
		// The runtime only terminates a replay for cancellations it issued (a restart, or the outer context).
		// A context the flow derived itself is the flow's concern: its cancellation must reach the flow's error handling.
		t.Run("cancelled before the step", func(t *testing.T) {
			// the step still runs: whether a cancelled context is an error is the step fn's decision
			var errs []error
			r, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
				ctx, cancel := context.WithCancel(b)
				defer cancel()
				if len(errs) == 0 {
					cancel()
				}
				v, err := futura.Step(b.WithContext(ctx), func(ctx context.Context, _ struct{}) (int, error) {
					if err := ctx.Err(); err != nil {
						return 0, err
					}
					return 1, nil
				}, struct{}{})
				if err != nil {
					errs = append(errs, err)
					return 0, err
				}
				return v, nil
			}, struct{}{})
			assert.NoError(t, err)
			assert.Equal(t, 1, r)
			assert.Len(t, errs, 1)
			assert.ErrorIs(t, errs[0], context.Canceled)
		})
		t.Run("cancelled during the step", func(t *testing.T) {
			// a timed-out step is a failed step: the flow sees the error and the loop retries it
			var errs []error
			r, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
				timeout := time.Millisecond
				if len(errs) > 0 {
					timeout = time.Second
				}
				ctx, cancel := context.WithTimeout(b, timeout)
				defer cancel()
				v, err := futura.Step(b.WithContext(ctx), func(ctx context.Context, _ struct{}) (int, error) {
					select {
					case <-ctx.Done():
						return 0, ctx.Err()
					case <-time.After(10 * time.Millisecond):
						return 1, nil
					}
				}, struct{}{})
				if err != nil {
					errs = append(errs, err)
					return 0, err
				}
				return v, nil
			}, struct{}{})
			assert.NoError(t, err)
			assert.Equal(t, 1, r)
			assert.Len(t, errs, 1)
			assert.ErrorIs(t, errs[0], context.DeadlineExceeded)
		})
	})
	t.Run("a step evaluated from inside another step is rejected", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		nest := true
		flowFn := func(b futura.FlowBuilder, _ struct{}) (int, error) {
			return futura.Step(b, func(ctx context.Context, _ struct{}) (int, error) {
				if nest {
					return futura.Step(b.WithContext(ctx), func(ctx context.Context, _ struct{}) (int, error) { return 0, nil }, struct{}{})
				}
				return 1, nil
			}, struct{}{})
		}
		_, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewStrict(c)).Execute(t.Context(), flowFn, struct{}{})
		assert.ErrorIs(t, err, futura.ErrFlowPanic)
		assert.ErrorIs(t, err, step.ErrNestedStep)

		// only the outer step's slot was recorded, so a corrected flow resumes cleanly over it
		nest = false
		r, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewStrict(c)).Execute(t.Context(), flowFn, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 1, r)
	})
	t.Run("a step re-executed with a new input memoizes that input, not the original one", func(t *testing.T) {
		replays := 0
		var outputs []int
		var executedWith []int
		_, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			replays++
			input := 1
			if replays == 2 {
				input = 2
			}
			out, err := futura.Step(b, func(ctx context.Context, in int) (int, error) {
				executedWith = append(executedWith, in)
				return in * 10, nil
			}, input)
			if err != nil {
				return 0, err
			}
			outputs = append(outputs, out)
			if replays < 3 {
				return 0, futura.Action(b, func(ctx context.Context) error { return errors.New("retry") })
			}
			return out, nil
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, []int{10, 20, 10}, outputs)
		assert.Equal(t, []int{1, 2, 1}, executedWith)
	})
	t.Run("a step re-executed with a new input hits the memo when the input repeats", func(t *testing.T) {
		// The input goes 1 -> 2 -> 2. The second replay with 2 must be a memo hit.
		replays := 0
		var executedWith []int
		_, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			replays++
			input := 1
			if replays >= 2 {
				input = 2
			}
			out, err := futura.Step(b, func(ctx context.Context, in int) (int, error) {
				executedWith = append(executedWith, in)
				return in * 10, nil
			}, input)
			if err != nil {
				return 0, err
			}
			if replays < 3 {
				return 0, futura.Action(b, func(ctx context.Context) error { return errors.New("retry") })
			}
			return out, nil
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, []int{1, 2}, executedWith)
	})
}
