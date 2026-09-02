package flow_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/futura-platform/futura/fopt"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/futura-platform/futura/internal/flow"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/flow/replay/sequence"
	"github.com/futura-platform/futura/internal/step"
	"github.com/futura-platform/futura/moment"
	"github.com/stretchr/testify/assert"
)

func loopAndAssertState[A, T any](t *testing.T, ctx context.Context, callableFlow flow.CallableFlow[A, T], args A, opts ...ftype.FlowLoopOption) (T, error) {
	t.Helper()
	// Loop expects an execution that's already running; mirror what Flow.Execute does.
	exec := execution.UnsafeFromContext(ctx)
	if exec == nil {
		t.Fatalf("no flow execution found in context")
	}
	stop, ok := exec.TryStartRun()
	if !ok {
		t.Fatalf("flow execution is already running")
	}
	defer stop()

	r, err := flow.Loop(ctx, callableFlow, args, opts...)
	assert.Panics(t, func() { sequence.GetIndex(ctx) })
	return r, err
}

func TestLoopFlow(t *testing.T) {
	t.Run("Basic flow", func(t *testing.T) {
		ctx := execution.WithFlow(t.Context(), execution.NewFlowExecution())
		rval, err := loopAndAssertState(t, ctx, func(ctx context.Context, _ *struct{}) (string, error) {
			return "test", nil
		}, &struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, "test", rval)
	})

	t.Run("Flow cancellation", func(t *testing.T) {
		ctx := execution.WithFlow(t.Context(), execution.NewFlowExecution())
		r, err := loopAndAssertState(t, ctx, func(ctx context.Context, _ *struct{}) (string, error) {
			_, err := step.Evaluate(ctx, moment.NewFn(func(ctx context.Context, _ struct{}) (string, error) {
				return "", ftype.ErrCancelFlow
			}), struct{}{})
			if err != nil {
				return "expected", err
			}
			return "unexpected", ftype.ErrCancelFlow
		}, &struct{}{})
		assert.ErrorIs(t, err, ftype.ErrCancelFlow)
		assert.Equal(t, "expected", r)
	})

	t.Run("Runs deferred functions when the flow succeeds", func(t *testing.T) {
		ctx := execution.WithFlow(t.Context(), execution.NewFlowExecution())
		var calls []int

		rval, err := loopAndAssertState(t, ctx, func(ctx context.Context, _ *struct{}) (string, error) {
			sequence.Defer(ctx, func() { calls = append(calls, 1) })
			sequence.Defer(ctx, func() { calls = append(calls, 2) })
			return "test", nil
		}, &struct{}{})

		assert.NoError(t, err)
		assert.Equal(t, "test", rval)
		assert.Equal(t, []int{2, 1}, calls)
	})

	t.Run("Runs deferred functions when the flow is cancelled", func(t *testing.T) {
		ctx := execution.WithFlow(t.Context(), execution.NewFlowExecution())
		calls := 0

		_, err := loopAndAssertState(t, ctx, func(ctx context.Context, _ *struct{}) (string, error) {
			sequence.Defer(ctx, func() { calls++ })
			return "", ftype.ErrCancelFlow
		}, &struct{}{})

		assert.ErrorIs(t, err, ftype.ErrCancelFlow)
		assert.Equal(t, 1, calls)
	})

	t.Run("Only runs deferred functions from the final replay", func(t *testing.T) {
		replayErr := errors.New("retry")
		ctx := execution.WithFlow(t.Context(), execution.NewFlowExecution())
		replays := 0
		calls := 0

		rval, err := loopAndAssertState(t, ctx, func(ctx context.Context, _ *struct{}) (string, error) {
			replays++
			sequence.Defer(ctx, func() { calls++ })
			if replays == 1 {
				return "", fmt.Errorf("%w: %w", step.ErrEvalFailed, replayErr)
			}
			return "test", nil
		}, &struct{}{})

		assert.NoError(t, err)
		assert.Equal(t, "test", rval)
		assert.Equal(t, 2, replays)
		assert.Equal(t, 1, calls)
	})

	t.Run("Regular error handling for evaluation failures (should retry)", func(t *testing.T) {
		testErr := errors.New("test error")
		ctx := execution.WithFlow(t.Context(), execution.NewFlowExecution())

		fnCallCount := 0
		rval, err := loopAndAssertState(t,
			ctx,
			func(ctx context.Context, _ *struct{}) (string, error) {
				fnCallCount++
				if fnCallCount >= 2 {
					return "result", nil
				}
				return "", fmt.Errorf("%w: %w", step.ErrEvalFailed, testErr)
			},
			&struct{}{},
		)
		assert.Equal(t, 2, fnCallCount)
		assert.Equal(t, "result", rval)
		assert.NoError(t, err)
	})

	t.Run("Regular error handling for non-evaluation failures (should not retry)", func(t *testing.T) {
		testErr := errors.New("test error")
		ctx := execution.WithFlow(t.Context(), execution.NewFlowExecution())
		fnCallCount := 0
		_, err := loopAndAssertState(t, ctx, func(ctx context.Context, _ *struct{}) (string, error) {
			fnCallCount++
			return "", testErr
		}, &struct{}{})
		assert.ErrorIs(t, err, testErr)
		assert.ErrorIs(t, err, flow.ErrOccurredOutsideOfEvaluation)
		assert.Equal(t, 1, fnCallCount)
	})

	t.Run("Context error handling", func(t *testing.T) {
		ctx := execution.WithFlow(t.Context(), execution.NewFlowExecution())
		ctx, cancel := context.WithCancel(ctx)
		_, err := loopAndAssertState(t, ctx, func(ctx context.Context, _ *struct{}) (string, error) {
			cancel()
			return "", errors.New("unrelated forever error")
		}, &struct{}{})
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("The loop should replay if the replay context was cancelled, even if the callable flow returns without an error", func(t *testing.T) {
		ctx := execution.WithFlow(t.Context(), execution.NewFlowExecution())
		// Loop hasn't started the run yet; reach for the exec via the unsafe accessor.
		f := execution.UnsafeFromContext(ctx)
		replays := 0
		rval, err := loopAndAssertState(t, ctx, func(ctx context.Context, _ *struct{}) (string, error) {
			if replays == 0 {
				f.InvalidateSequence(ctx)
				f.RestartCurrentReplay(ctx, errors.New("replay cancelled"))
			}
			replays++
			return fmt.Sprintf("success on replay %d", replays), nil
		}, &struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, "success on replay 2", rval)
	})

	t.Run("step memos should be stable after a branch is closed and reopened", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		ctx := execution.WithFlow(t.Context(), execution.NewFlowExecutionWithContainer(c))

		replays := 0
		stepCalls := 0
		fn := moment.NewFn(func(ctx context.Context, _ struct{}) (string, error) {
			stepCalls++
			return fmt.Sprintf("fn1 on replay %d", replays), nil
		}, ftype.WithLabel("fn"))

		f := execution.UnsafeFromContext(ctx)
		callOrderLengths := make([]int, 0)
		r, err := loopAndAssertState(t, ctx, func(ctx context.Context, _ struct{}) (r string, err error) {
			replays++
			callOrderLengths = append(callOrderLengths, c.CallOrderLength())

			// the branch closes on replay 2, and reopens on replay 4. we must signal the change here
			if replays == 2 || replays == 4 {
				f.InvalidateSequence(ctx)
				f.RestartCurrentReplay(ctx, errors.New("skipping the branch on this replay"))
			}
			// skip the step on the second+third replay to allow the invalidation to settle, then to actually skip the branch
			if replays != 2 && replays != 3 {
				_, err = step.Evaluate(ctx, fn, struct{}{})
				if err != nil {
					return "", err
				}
			}

			// retry until the fn will have executed twice
			return step.Evaluate(ctx, moment.NewFn(func(ctx context.Context, _ struct{}) (string, error) {
				if replays < 6 {
					return "", fmt.Errorf("retry trigger")
				}
				return fmt.Sprintf("success on replay %d", replays), nil
			}, ftype.WithLabel("success fn")), struct{}{})
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 6, replays)
		assert.Equal(t, "success on replay 6", r)
		assert.Equal(t, 1, stepCalls)

		t.Run("the call order should be truncated after the branch is closed", func(t *testing.T) {
			switch replays {
			case 2:
				assert.Equal(t, 2, c.CallOrderLength())
			case 4:
				assert.Equal(t, 1, c.CallOrderLength())
			case 6:
				assert.Equal(t, 2, c.CallOrderLength())
			}
		})
	})

	t.Run("End to end flow with steps", func(t *testing.T) {
		errCount := 0
		expectedErr := errors.New("test error")
		ctx := execution.WithFlow(t.Context(), execution.NewFlowExecution())

		fn1Calls := 0
		failsTwice := moment.NewFn(func(ctx context.Context, _ *any) (string, error) {
			fn1Calls++
			if fn1Calls <= 2 {
				return "", expectedErr
			}
			return "fn1", nil
		}, ftype.WithLabel("failsTwice"))

		fn2 := moment.NewFn(func(ctx context.Context, _ *any) (string, error) {
			return "fn2", nil
		}, ftype.WithLabel("fn2"))

		rval, err := loopAndAssertState(t, ctx, func(ctx context.Context, _ *any) (string, error) {
			// todo: make this include a conditional branch to cover moment eviction.
			r1, err := step.Evaluate(ctx, failsTwice, nil)
			if err != nil {
				return "", err
			}

			r2, err := step.Evaluate(ctx, fn2, nil)
			if err != nil {
				return "", err
			}
			return r1 + r2, nil
		}, nil, fopt.WithOnStepError(func(ctx context.Context, fnLabel string, callstack []runtime.Frame, err error) (continueExecution bool) {
			assert.ErrorIs(t, err, expectedErr)
			assert.Equal(t, "failsTwice", fnLabel)
			assert.NotEmpty(t, callstack)
			errCount++
			return true
		}))
		assert.NoError(t, err)
		assert.Equal(t, "fn1fn2", rval)
		assert.Equal(t, 2, errCount)
		assert.Equal(t, 3, fn1Calls)
	})

	t.Run("Applies context wrappers in order", func(t *testing.T) {
		const collidingKey = "collidingKey"
		wrapper1Value := "wrapper1"
		wrapper2Value := "wrapper2"
		ctx := execution.WithFlow(t.Context(), execution.NewFlowExecution())
		loopAndAssertState(t, ctx, func(ctx context.Context, _ *struct{}) (string, error) {
			assert.Equal(t, wrapper2Value, ctx.Value(collidingKey))
			return "test", nil
		}, &struct{}{},
			func(ctx context.Context) context.Context {
				return context.WithValue(ctx, collidingKey, wrapper1Value)
			},
			func(ctx context.Context) context.Context {
				return context.WithValue(ctx, collidingKey, wrapper2Value)
			},
		)
	})

	t.Run("New branches can be taken if the dirty epoch is ahead of the evaluated epoch", func(t *testing.T) {
		f := execution.NewFlowExecution()
		ctx := execution.WithFlow(t.Context(), f)
		replays := 0
		newFirstCalls := 0
		newSecondCalls := 0
		rval, err := loopAndAssertState(t, ctx, func(ctx context.Context, _ *struct{}) (string, error) {
			replays++
			if replays > 1 {
				// try each of the second calls with a replay each,
				// to make sure that the new branch is valid across all replays after the first time it is taken.
				_, err := step.Evaluate(ctx, moment.NewFn(func(ctx context.Context, _ struct{}) (string, error) {
					newFirstCalls++
					if newFirstCalls == 1 {
						return "", errors.New("retry trigger")
					}
					return "success", nil
				}, ftype.WithLabel("new first call")), struct{}{})
				if err != nil {
					return "", err
				}

				_, err = step.Evaluate(ctx, moment.NewFn(func(ctx context.Context, _ struct{}) (string, error) {
					newSecondCalls++
					if newSecondCalls == 1 {
						return "", errors.New("retry trigger")
					}
					return "success", nil
				}, ftype.WithLabel("new second call")), struct{}{})
				if err != nil {
					return "", err
				}
				return fmt.Sprintf("success on replay %d", replays), nil
			}

			_, err := step.Evaluate(ctx, moment.NewFn(func(ctx context.Context, _ struct{}) (string, error) {
				return "success", nil
			}, ftype.WithLabel("old first call")), struct{}{})
			if err != nil {
				return "", err
			}

			_, err = step.Evaluate(ctx, moment.NewFn(func(ctx context.Context, _ struct{}) (string, error) {
				return "success", nil
			}, ftype.WithLabel("old second call")), struct{}{})
			if err != nil {
				return "", err
			}

			f.InvalidateSequence(ctx)
			f.RestartCurrentReplay(ctx, errors.New("restart to enter new branch"))
			return "", nil
		}, &struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, "success on replay 4", rval)
	})

	t.Run("terminal exits do not settle the sequence", func(t *testing.T) {
		assertDoesNotSettle := func(t *testing.T, expectedErr error, exit func(ctx context.Context, cancel context.CancelFunc) error) {
			t.Helper()

			c := executiontype.NewInMemoryContainer()
			outerCtx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctx := execution.WithFlow(outerCtx, execution.NewFlowExecutionWithContainer(c))
			f := execution.UnsafeFromContext(ctx)

			replays := 0
			_, err := loopAndAssertState(t, ctx, func(ctx context.Context, _ struct{}) (string, error) {
				replays++
				if _, err := step.Evaluate(ctx, moment.NewFn(func(ctx context.Context, _ struct{}) (string, error) {
					return "success", nil
				}, ftype.WithLabel("first call")), struct{}{}); err != nil {
					return "", err
				}
				if replays == 1 {
					f.InvalidateSequence(ctx)
					f.RestartCurrentReplay(ctx, errors.New("state change"))
					return "", nil
				}
				return "", exit(ctx, cancel)
			}, struct{}{})
			assert.ErrorIs(t, err, expectedErr)
			assert.Equal(t, 2, replays)

			// resume over the same container, taking a different branch than was recorded.
			resumedCtx := execution.WithFlow(t.Context(), execution.NewFlowExecutionWithContainer(c))
			rval, err := loopAndAssertState(t, resumedCtx, func(ctx context.Context, _ struct{}) (string, error) {
				return step.Evaluate(ctx, moment.NewFn(func(ctx context.Context, _ struct{}) (string, error) {
					return "new branch", nil
				}, ftype.WithLabel("new branch call")), struct{}{})
			}, struct{}{})
			assert.NoError(t, err)
			assert.Equal(t, "new branch", rval)
		}

		t.Run("cancel flow error", func(t *testing.T) {
			assertDoesNotSettle(t, ftype.ErrCancelFlow, func(context.Context, context.CancelFunc) error {
				return ftype.ErrCancelFlow
			})
		})
		t.Run("non-evaluation error", func(t *testing.T) {
			assertDoesNotSettle(t, flow.ErrOccurredOutsideOfEvaluation, func(context.Context, context.CancelFunc) error {
				return errors.New("outside of a step")
			})
		})
		t.Run("outer context cancellation", func(t *testing.T) {
			assertDoesNotSettle(t, context.Canceled, func(_ context.Context, cancel context.CancelFunc) error {
				cancel()
				return errors.New("cancelled by the outer context")
			})
		})
	})
}
