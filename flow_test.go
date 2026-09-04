package futura_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/fopt"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/step"
	"github.com/futura-platform/futura/internal/utils/containertest"
	"github.com/stretchr/testify/assert"
)

func TestFlow(t *testing.T) {
	t.Run("basic e2e test", func(t *testing.T) {
		f := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory())
		r, err := f.Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
			return "result", nil
		}, nil)
		assert.NoError(t, err)
		assert.Equal(t, "result", r)
	})
	t.Run("do not call Flow from within a flow", func(t *testing.T) {
		outerExec := execution.NewFlowExecutionWithContainer(containertest.NewInMemory())
		stop, _ := outerExec.TryStartRun()
		defer stop()
		ctx := execution.WithFlow(t.Context(), outerExec)
		f1 := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory())
		f2 := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory())
		_, err := f1.Execute(ctx, func(b futura.FlowBuilder, _ *any) (string, error) {
			f2.Execute(ctx, func(b futura.FlowBuilder, _ *any) (string, error) {
				return "never reached 1", nil
			}, nil)
			return "never reached 2", nil
		}, nil)
		assert.ErrorIs(t, err, futura.ErrTopLevelFlowConflict)
	})
	t.Run("a flow context from an execution that is not running is reported as a flow panic", func(t *testing.T) {
		// the conflict check asserts on the context it is given; that assertion must be recovered like any other
		stale := execution.WithFlow(t.Context(), execution.NewFlowExecutionWithContainer(containertest.NewInMemory()))
		var err error
		assert.NotPanics(t, func() {
			_, err = futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(stale, func(b futura.FlowBuilder, _ *any) (string, error) {
				return "never reached", nil
			}, nil)
		})
		assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
		assert.ErrorIs(t, err, execution.ErrFlowExecutionNotRunning)
	})
	t.Run("a builder from a replay that has ended cannot be used for a step", func(t *testing.T) {
		// Its replay is over, so a step through it has nothing to record into. It must be rejected,
		// never reported as a clean completion of the replay that is actually running.
		useStale := func(t *testing.T, endFirstReplay func(b futura.FlowBuilder) error) error {
			t.Helper()
			var stale *futura.FlowBuilder
			ran, replays := false, 0
			r, err := futura.NewFlowFromContainer[*any, int](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (int, error) {
				replays++
				if replays == 1 {
					held := b
					stale = &held
					return 0, endFirstReplay(b)
				}
				v, err := futura.Step(*stale, func(ctx context.Context, _ struct{}) (int, error) {
					ran = true
					return 42, nil
				}, struct{}{})
				if err != nil {
					return -1, err
				}
				return v, nil
			}, nil)
			assert.False(t, ran)
			assert.Equal(t, 0, r)
			return err
		}
		t.Run("ended by a restart", func(t *testing.T) {
			err := useStale(t, func(b futura.FlowBuilder) error {
				futura.State(b, false).Set(true)
				return nil
			})
			assert.ErrorIs(t, err, flow.ErrStaleReplay)
		})
		t.Run("ended by a failed step", func(t *testing.T) {
			err := useStale(t, func(b futura.FlowBuilder) error {
				return futura.Action(b, func(ctx context.Context) error { return errors.New("retry") })
			})
			assert.ErrorIs(t, err, flow.ErrStaleReplay)
			assert.ErrorIs(t, err, flow.ErrReplayEnded)
		})
		t.Run("ended by a panic", func(t *testing.T) {
			// a replay that unwinds by panic has ended just the same: its builder is dead, and every
			// context derived from it is done
			f := futura.NewFlowFromContainer[*any, int](containertest.NewInMemory())
			var stale futura.FlowBuilder
			var stepCtx context.Context
			_, err := f.Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (int, error) {
				stale = b
				_, _ = futura.Source(b, func(ctx context.Context) (int, error) { stepCtx = ctx; return 1, nil })
				panic("first execution panics")
			}, nil)
			assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
			assert.ErrorIs(t, context.Cause(stepCtx), flow.ErrReplayEnded)

			ran := false
			r, err := f.Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (int, error) {
				return futura.Step(stale, func(ctx context.Context, _ struct{}) (int, error) { ran = true; return 42, nil }, struct{}{})
			}, nil)
			assert.ErrorIs(t, err, flow.ErrStaleReplay)
			assert.False(t, ran)
			assert.Equal(t, 0, r)
		})
	})
	t.Run("do not execute a flow more than once concurrently", func(t *testing.T) {
		fnEntered := make(chan struct{})
		goroutine2Finished := make(chan struct{})
		fn := func(b futura.FlowBuilder, _ *any) (string, error) {
			close(fnEntered)
			<-goroutine2Finished
			return "result", nil
		}
		f := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory())
		go func() {
			<-fnEntered
			defer close(goroutine2Finished)
			_, err := f.Execute(t.Context(), fn, nil)
			assert.ErrorIs(t, err, futura.ErrAlreadyRunning)
		}()
		r, err := f.Execute(t.Context(), fn, nil)
		assert.NoError(t, err)
		assert.Equal(t, "result", r)
	})
	t.Run("a panic in a deferred function is reported like any other, after the flow has ended", func(t *testing.T) {
		deferErr := errors.New("defer boom")
		var seenByHook error
		hookErr := errors.New("hook error")
		r, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			futura.Defer(b, func() { panic(deferErr) })
			return 42, nil
		}, struct{}{}, fopt.WithOnExecutionEnd(func(_ context.Context, err error) error {
			seenByHook = err
			return hookErr
		}))
		assert.Equal(t, 42, r, "the flow's result is not affected by its deferred functions")
		assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
		assert.ErrorIs(t, err, deferErr)
		assert.ErrorIs(t, err, hookErr, "the end hook's error is joined")
		assert.ErrorIs(t, seenByHook, deferErr, "the end hook sees the flow's error")

		sentinel := errors.New("the flow's own reason")
		_, err = futura.NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			futura.Defer(b, func() { panic(deferErr) })
			return 0, fmt.Errorf("%w: %w", ftype.ErrCancelFlow, sentinel)
		}, struct{}{})
		assert.ErrorIs(t, err, ftype.ErrCancelFlow, "the flow's own error survives")
		assert.ErrorIs(t, err, sentinel)
		assert.ErrorIs(t, err, deferErr)
	})
	t.Run("Flow recovers from panics", func(t *testing.T) {
		var expectedErr = errors.New("expected panic")
		_, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
			panic(expectedErr)
		}, nil)
		assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
		assert.ErrorIs(t, err, expectedErr)
		assert.Contains(t, err.Error(), "flow_test.go", "the panic's stack is attached")
	})
	t.Run("a panic in a step names the step", func(t *testing.T) {
		_, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
			return futura.Source(b, func(ctx context.Context) (string, error) {
				var s []int
				return "", fmt.Errorf("%d", s[3])
			}, ftype.WithLabel("theStepThatPanicked"))
		}, nil)
		assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
		assert.Contains(t, err.Error(), "theStepThatPanicked")
		assert.Contains(t, err.Error(), "index out of range")
		assert.Contains(t, err.Error(), "flow_test.go", "the panic's stack is attached")
	})
	t.Run("Flow recovers from panics with non-error values", func(t *testing.T) {
		_, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
			panic("not an error type")
		}, nil)
		assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
		assert.Contains(t, err.Error(), "not an error type")
	})

	fn1 := func(ctx context.Context, args *any) (string, error) {
		return "fn1", errors.New("expected error")
	}
	fn2 := func(ctx context.Context, args *any) (string, error) {
		return "fn2", nil
	}
	checkMultipleMomentFunctions := func(t *testing.T, onUseFn1 func(futura.FlowBuilder) futura.FlowBuilder, onUseFn2 func(futura.FlowBuilder) futura.FlowBuilder) (string, error) {
		replayCount := 0
		r, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
			var vfn futura.ComparableMomentFn[*any, string]
			if replayCount == 0 {
				vfn = fn1
				if onUseFn1 != nil {
					b = onUseFn1(b)
				}
			} else {
				vfn = fn2
				if onUseFn2 != nil {
					b = onUseFn2(b)
				}
			}
			replayCount++
			return futura.Step(b, vfn, nil)
		}, nil)
		assert.Equal(t, 2, replayCount)
		return r, err
	}
	t.Run("A single keyless moment identity should only ever be used with a single moment function", func(t *testing.T) {
		// the fn is part of the identity, so swapping it at a callsite is a new branch, which a strict replay rejects
		_, err := checkMultipleMomentFunctions(t, nil, nil)
		assert.ErrorIs(t, err, ftrerrors.ErrInconsistentState)
		assert.ErrorIs(t, err, step.ErrUnexpectedBranchTaken)
	})
	t.Run("A keyed moment identity can be used with a different moment function once a state change relaxes the replay", func(t *testing.T) {
		// the key is part of the identity, so changing it at a settled index is a new branch:
		// it is allowed for the same reason any new branch is, a state change
		r, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
			useSecond := futura.State(b, false)
			if !useSecond.V() {
				if _, err := futura.Step(b.WithKey("1"), fn1, nil); err != nil {
					useSecond.Set(true)
					return "", err
				}
			}
			return futura.Step(b.WithKey("2"), fn2, nil)
		}, nil)
		assert.NoError(t, err)
		assert.Equal(t, "fn2", r)
	})
	t.Run("a key that changes at a settled index without a state change is an unexpected branch", func(t *testing.T) {
		replays := 0
		_, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
			replays++
			// a key that is not a pure function of the flow's state
			return futura.Step(b.WithKey(strconv.Itoa(replays)), fn1, nil)
		}, nil)
		assert.ErrorIs(t, err, step.ErrUnexpectedBranchTaken)
	})
	t.Run("a loop keyed by its index is memoized across replays", func(t *testing.T) {
		calls := 0
		replays := 0
		r, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
			replays++
			for i := range 3 {
				if err := futura.Action(b.WithKey(strconv.Itoa(i)), func(ctx context.Context) error { calls++; return nil }); err != nil {
					return "", err
				}
			}
			if replays < 3 {
				return "", futura.Action(b, func(ctx context.Context) error { return errors.New("retry") })
			}
			return "done", nil
		}, nil)
		assert.NoError(t, err)
		assert.Equal(t, "done", r)
		assert.Equal(t, 3, replays)
		assert.Equal(t, 3, calls, "each keyed iteration ran once, then hit its memo on every replay")
	})
	t.Run("layered keys do not collide with each other or with a single key", func(t *testing.T) {
		calls := 0
		r, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
			step := func(b futura.FlowBuilder) error {
				return futura.Action(b, func(ctx context.Context) error { calls++; return nil })
			}
			for _, kb := range []futura.FlowBuilder{
				b.WithKey("a-b").WithKey("c"),
				b.WithKey("a").WithKey("b-c"),
				b.WithKey("a-b-c"),
			} {
				if err := step(kb); err != nil {
					return "", err
				}
			}
			return "done", nil
		}, nil)
		assert.NoError(t, err)
		assert.Equal(t, "done", r)
		assert.Equal(t, 3, calls)
	})
	t.Run("A single keyed moment identity should be able to be used with a single moment function, and have memoization keyed by the identity key", func(t *testing.T) {
		expectedExecCount := 10
		execCount := 0
		fn := func(ctx context.Context, _ *struct{}) error {
			execCount++
			return nil
		}
		_, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).
			Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
				for i := range expectedExecCount {
					b = b.WithKey(strconv.Itoa(i))
					err := futura.Effect(b, fn, nil)
					assert.NoError(t, err)
				}
				return "", nil
			}, nil)
		assert.NoError(t, err)
		assert.Equal(t, expectedExecCount, execCount)
	})
	t.Run("Flow can be persisted in an execution container, so that the execution state resumes from where it left off", func(t *testing.T) {
		step1Called := make(chan struct{})
		defer close(step1Called)

		step1Calls := 0
		step2Calls := 0

		step1 := func(_ context.Context, _ struct{}) (string, error) {
			step1Calls++
			step1Called <- struct{}{}
			return "step1", nil
		}
		step2 := func(_ context.Context, _ struct{}) (string, error) {
			step2Calls++
			return "step2", nil
		}
		flowFn := func(b futura.FlowBuilder, _ *any) (string, error) {
			r1, err := futura.Step(b, step1, struct{}{})
			if err != nil {
				return "", err
			}
			// give the context time to be cancelled
			time.Sleep(time.Millisecond * 100)
			r2, err := futura.Step(b, step2, struct{}{})
			if err != nil {
				return "", err
			}
			return r1 + r2, nil
		}

		container := executiontype.NewInMemoryContainer()

		// perform the first execution
		f1 := futura.NewFlowFromContainer[*any, string](containertest.NewStrict(container))

		firstExecutionContext, cancelFirstExecution := context.WithCancel(t.Context())
		defer cancelFirstExecution()

		go func() {
			<-step1Called
			cancelFirstExecution()
		}()

		_, err := f1.Execute(firstExecutionContext, flowFn, nil)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, 1, step1Calls)
		assert.Equal(t, 0, step2Calls)

		// simulate a context switch
		f2 := futura.NewFlowFromContainer[*any, string](containertest.NewStrict(container))

		// resume the execution
		_, err = f2.Execute(t.Context(), flowFn, nil)
		assert.NoError(t, err)
		assert.Equal(t, 1, step1Calls)
		assert.Equal(t, 1, step2Calls)
	})
}
