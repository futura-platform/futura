package fopt_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/fopt"
	"github.com/futura-platform/futura/ftype/executiontype"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/utils/containertest"
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
		_, err := futura.NewFlowFromContainer[any, any](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, args any) (any, error) {
			return futura.Step(b, failsTwice, nil)
		}, nil, fopt.WithMaxFailures(2))
		require.NoError(t, err)
	})

	t.Run("max failures reached", func(t *testing.T) {
		failsTwice := failsNTimesStep(2)
		_, err := futura.NewFlowFromContainer[any, any](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, args any) (any, error) {
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
		f := futura.NewFlowFromContainer[any, any](containertest.NewStrict(container))
		firstExecCtx, firstExecCancel := context.WithCancel(t.Context())
		_, err := exec2FailStepTest(firstExecCtx, f, firstExecCancel)
		require.ErrorIs(t, err, firstExecCtx.Err())

		// second execution (fails again once, which should trigger the max failures error)
		f = futura.NewFlowFromContainer[any, any](containertest.NewStrict(container))
		_, err = exec2FailStepTest(t.Context(), f, func() {})
		require.ErrorIs(t, err, fopt.ErrMaxFailuresReached)
	})

	t.Run("a failure returned after a state change inside the step is counted", func(t *testing.T) {
		_, err := futura.NewFlowFromContainer[any, any](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, args any) (any, error) {
			phase := futura.State(b, 0)
			return nil, futura.Action(b, func(ctx context.Context) error {
				phase.Set(phase.V() + 1)
				return errors.New("always")
			})
		}, nil, fopt.WithMaxFailures(2))
		require.ErrorIs(t, err, fopt.ErrMaxFailuresReached)
	})
	t.Run("a state change made in response to the cancellation does not restart the flow", func(t *testing.T) {
		// Reaching the limit ends the flow. A flow that records the failure in a state (which restarts
		// the replay) must still end, otherwise every replay reaches the limit again and it never does.
		ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		defer cancel()
		replays := 0
		_, err := futura.NewFlowFromContainer[any, any](containertest.NewInMemory()).Execute(ctx, func(b futura.FlowBuilder, args any) (any, error) {
			replays++
			failures := futura.State(b, 0)
			err := futura.Action(b, func(ctx context.Context) error { return errors.New("always") })
			if err != nil {
				failures.Set(failures.V() + 1)
				return nil, err
			}
			return nil, nil
		}, nil, fopt.WithMaxFailures(2))
		require.ErrorIs(t, err, fopt.ErrMaxFailuresReached)
		require.NotErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 3, replays)
	})
	t.Run("a wrapper that recovers panics cannot turn a replay's termination into a failure", func(t *testing.T) {
		// a replay is terminated by a panic raised inside the wrapper's frame. A wrapper that recovers
		// panics (annotating middleware) hands back an error instead; the runtime must still see the
		// termination for what it is, not count it as a failure.
		annotating := func(ctx context.Context, fnLabel string, args any, callstack []runtime.Frame, call func() (any, error)) (errOverride error) {
			defer func() {
				if r := recover(); r != nil {
					errOverride = fmt.Errorf("step %s panicked: %v", fnLabel, r)
				}
			}()
			_, err := call()
			return err
		}
		r, err := futura.NewFlowFromContainer[any, int](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ any) (int, error) {
			s := futura.State(b, 0)
			return futura.Step(b, func(ctx context.Context, _ *struct{}) (int, error) {
				if v := s.V(); v < 3 {
					s.Set(v + 1) // restarts the replay; the step is terminated
					return 0, ctx.Err()
				}
				return s.V(), nil
			}, nil)
		}, nil, fopt.WithMaxFailures(2), fopt.WithStepWrapper(annotating))
		require.NoError(t, err)
		require.Equal(t, 3, r)
	})
	t.Run("a wrapper that recovers a step's own panic after a state change reports the panic, not a restart", func(t *testing.T) {
		annotating := func(ctx context.Context, fnLabel string, args any, callstack []runtime.Frame, call func() (any, error)) (errOverride error) {
			defer func() {
				if r := recover(); r != nil {
					errOverride = fmt.Errorf("step %s panicked: %v", fnLabel, r)
				}
			}()
			_, err := call()
			return err
		}
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		replays := 0
		_, err := futura.NewFlowFromContainer[any, int](containertest.NewInMemory()).Execute(ctx, func(b futura.FlowBuilder, _ any) (int, error) {
			replays++
			s := futura.State(b, 0)
			return futura.Step(b, func(ctx context.Context, _ *struct{}) (int, error) {
				s.Set(s.V() + 1) // the replay is now cancelled for a restart
				panic("a real bug in the step")
			}, nil)
		}, nil, fopt.WithStepWrapper(annotating))
		require.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
		require.ErrorContains(t, err, "a real bug in the step")
		require.NotErrorIs(t, err, context.DeadlineExceeded, "the flow restarted forever instead of reporting the panic")
		require.Less(t, replays, 10)
	})
	t.Run("a wrapper that re-panics a termination wrapped in its own error still restarts the replay", func(t *testing.T) {
		rewrapping := func(ctx context.Context, fnLabel string, args any, callstack []runtime.Frame, call func() (any, error)) (errOverride error) {
			defer func() {
				if r := recover(); r != nil {
					if e, ok := r.(error); ok {
						panic(fmt.Errorf("step %s: %w", fnLabel, e))
					}
					panic(r)
				}
			}()
			_, err := call()
			return err
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
		}, nil, fopt.WithStepWrapper(rewrapping))
		require.NoError(t, err)
		require.Equal(t, 3, r)
	})
	t.Run("a wrapper that swallows a panic cannot memoize a step that never returned", func(t *testing.T) {
		swallowing := func(ctx context.Context, fnLabel string, args any, callstack []runtime.Frame, call func() (any, error)) (errOverride error) {
			defer func() { recover() }()
			call()
			return nil
		}
		c := containertest.NewInMemory()
		calls := 0
		flowFn := func(b futura.FlowBuilder, _ any) (int, error) {
			return futura.Step(b, func(ctx context.Context, _ *struct{}) (int, error) { calls++; panic("boom") }, nil)
		}
		_, err := futura.NewFlowFromContainer[any, int](c).Execute(t.Context(), flowFn, nil, fopt.WithStepWrapper(swallowing))
		require.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
		// the step was never recorded as done, so a fresh execution runs it again
		_, err = futura.NewFlowFromContainer[any, int](c).Execute(t.Context(), flowFn, nil, fopt.WithStepWrapper(swallowing))
		require.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
		require.Equal(t, 2, calls)
	})
	t.Run("a wrapper that returns before the call does cannot memoize an output the step never produced", func(t *testing.T) {
		release := make(chan struct{})
		defer close(release)
		abandoning := func(ctx context.Context, fnLabel string, args any, callstack []runtime.Frame, call func() (any, error)) (errOverride error) {
			started := make(chan struct{})
			go func() {
				close(started)
				call()
			}()
			<-started
			return nil
		}
		c := executiontype.NewInMemoryContainer()
		flowFn := func(b futura.FlowBuilder, _ any) (string, error) {
			return futura.Step(b, func(ctx context.Context, _ *struct{}) (string, error) { <-release; return "real", nil }, nil)
		}
		_, err := futura.NewFlowFromContainer[any, string](containertest.NewStrict(c)).Execute(t.Context(), flowFn, nil, fopt.WithStepWrapper(abandoning))
		require.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
		require.Equal(t, 0, c.CallOrderLength(), "the step was recorded")
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
			_, err := futura.NewFlowFromContainer[any, any](containertest.NewStrict(container)).Execute(ctx, flowFn, nil, fopt.WithMaxFailures(2))
			require.ErrorIs(t, err, context.Canceled)
		}

		// a healthy execution with one real failure must still be within budget
		cancelDuringStep = nil
		_, err := futura.NewFlowFromContainer[any, any](containertest.NewStrict(container)).Execute(t.Context(), flowFn, nil, fopt.WithMaxFailures(2))
		require.NoError(t, err)
	})
}
