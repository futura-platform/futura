package fopt_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/fopt"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/stretchr/testify/require"
)

func TestWithMaxFailures(t *testing.T) {
	failsNTimesStep := func(n int32) futura.ComparableMomentFn[*struct{}, *struct{}] {
		var count atomic.Int32
		return func(ctx context.Context, args *struct{}) (*struct{}, error) {
			if count.Add(1) <= n {
				return nil, errors.New("test error")
			}
			return nil, nil
		}
	}

	t.Run("max failures undershot", func(t *testing.T) {
		failsTwice := failsNTimesStep(2)
		_, err := futura.NewFlow[any, any]().Execute(t.Context(), func(b futura.FlowBuilder, args any) (any, error) {
			return futura.Step(b, failsTwice, nil)
		}, nil, fopt.WithMaxFailures(2))
		require.NoError(t, err)
	})

	t.Run("max failures reached", func(t *testing.T) {
		failsTwice := failsNTimesStep(2)
		_, err := futura.NewFlow[any, any]().Execute(t.Context(), func(b futura.FlowBuilder, args any) (any, error) {
			return futura.Step(b, failsTwice, nil)
		}, nil, fopt.WithMaxFailures(1))
		require.ErrorIs(t, err, fopt.ErrMaxFailuresReached)
	})

	t.Run("failure count is tracked durably", func(t *testing.T) {
		container := executiontype.NewInMemoryContainer()

		// step fails twice.
		// we set it up like this so that our flow is pure. Future detects this and will panic if it isnt.
		step := failsNTimesStep(2)
		exec2FailStepTest := func(ctx context.Context, f *futura.Flow[any, any], afterStep func()) (any, error) {
			return f.Execute(ctx, func(b futura.FlowBuilder, args any) (any, error) {
				defer afterStep()
				return futura.Step(b, step, nil)
			}, nil, fopt.WithMaxFailures(1))
		}

		// first execution (fails once, which should increment the failure count to 1, not triggering the max failures error yet)
		f := futura.NewFlowFromContainer[any, any](container)
		firstExecCtx, firstExecCancel := context.WithCancel(t.Context())
		_, err := exec2FailStepTest(firstExecCtx, f, firstExecCancel)
		require.ErrorIs(t, err, firstExecCtx.Err())

		// second execution (fails again once, which should trigger the max failures error)
		f = futura.NewFlowFromContainer[any, any](container)
		_, err = exec2FailStepTest(t.Context(), f, func() {})
		require.ErrorIs(t, err, fopt.ErrMaxFailuresReached)
	})

	t.Run("a cancellation that lands during a step is not counted as a failure", func(t *testing.T) {
		container := executiontype.NewInMemoryContainer()
		var cancelDuringStep context.CancelFunc
		realFailures := 0
		flowFn := func(b futura.FlowBuilder, _ any) (any, error) {
			return futura.Step(b, func(ctx context.Context, _ *struct{}) (*struct{}, error) {
				if cancelDuringStep != nil {
					cancelDuringStep()
					return nil, ctx.Err()
				}
				if realFailures < 1 {
					realFailures++
					return nil, errors.New("transient")
				}
				return nil, nil
			}, nil)
		}

		// two executions are cancelled from inside the step
		for range 2 {
			ctx, cancel := context.WithCancel(t.Context())
			cancelDuringStep = cancel
			_, err := futura.NewFlowFromContainer[any, any](container).Execute(ctx, flowFn, nil, fopt.WithMaxFailures(2))
			require.ErrorIs(t, err, context.Canceled)
		}

		// a healthy execution with one real failure must still be within budget
		cancelDuringStep = nil
		_, err := futura.NewFlowFromContainer[any, any](container).Execute(t.Context(), flowFn, nil, fopt.WithMaxFailures(2))
		require.NoError(t, err)
	})
}
