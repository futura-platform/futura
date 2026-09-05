package step

import (
	"context"
	"errors"
	"runtime"

	"testing"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/flow/replay"
	"github.com/futura-platform/futura/internal/flow/replay/sequence"
	"github.com/futura-platform/futura/internal/utils/containertest"
	"github.com/futura-platform/futura/internal/utils/testutil"
	"github.com/futura-platform/futura/moment"

	"github.com/stretchr/testify/assert"
)

var mockStableCallstack = []runtime.Frame{{
	File: "mock.go",
	Line: 1,
}}

// runningFlowCtx returns a context with a fresh, running FlowExecution attached.
// The run is automatically stopped at the end of the test.
func runningFlowCtx(t *testing.T) context.Context {
	t.Helper()
	exec := execution.NewFlowExecutionWithContainer(containertest.NewInMemory())
	stop, ok := exec.TryStartRun()
	if !ok {
		t.Fatalf("flow execution is already running")
	}
	t.Cleanup(stop)
	return execution.WithFlow(t.Context(), exec)
}

// replayTerminatedWith runs fn as a replay and returns the panic value that terminated it.
// A replay that returns normally, or panics with a non-error, fails the test: termination
// is the only way fn is expected to end.
func replayTerminatedWith(t *testing.T, ctx context.Context, fn func(ctx context.Context)) error {
	t.Helper()
	var terminatedWith error
	result, err := replay.Execute(ctx, func(ctx context.Context, args any) (any, error) {
		defer func() {
			r, ok := recover().(error)
			if !ok {
				t.Fatalf("replay ended with a non-error panic: %v", r)
			}
			terminatedWith = r
		}()
		fn(ctx)
		return "returned normally", nil
	}, nil)
	assert.NoError(t, err)
	assert.Nil(t, result, "replay should have terminated, not returned normally")
	return terminatedWith
}

func TestStep(t *testing.T) {
	t.Run("memoize result for identical inputs", func(t *testing.T) {
		result, err := replay.Execute(runningFlowCtx(t), func(ctx context.Context, args any) (any, error) {
			f := execution.MustFromContext(ctx)
			ctx, _ = f.StartNewReplay(ctx)

			expectedResult := "expectedResult"
			callCount := 0
			fn := moment.NewFn(func(ctx context.Context, _ struct{}) (string, error) {
				callCount++
				return expectedResult, nil
			})

			result1, err := evaluateWithCallstack(ctx, fn, struct{}{}, mockStableCallstack)
			assert.NoError(t, err)
			assert.Equal(t, expectedResult, result1)
			assert.Equal(t, 1, sequence.GetIndex(ctx))
			assert.Equal(t, 1, callCount)

			ctx = sequence.With(ctx, replay.Flags{}) // rewind
			result2, err := evaluateWithCallstack(ctx, fn, struct{}{}, mockStableCallstack)
			assert.NoError(t, err)
			assert.Equal(t, result1, result2)
			assert.Equal(t, 1, sequence.GetIndex(ctx))
			assert.Equal(t, 1, callCount)
			return "success", nil
		}, nil)
		assert.NoError(t, err)
		assert.Equal(t, "success", result)
	})

	t.Run("the moment fn can read its own identity, and only while it executes", func(t *testing.T) {
		result, err := replay.Execute(runningFlowCtx(t), func(ctx context.Context, args any) (any, error) {
			f := execution.MustFromContext(ctx)
			ctx, _ = f.StartNewReplay(ctx)

			fn := moment.NewFn(func(ctx context.Context, _ struct{}) (moment.Identity, error) {
				return moment.CurrentIdentity(ctx), nil
			})
			expected := moment.NewIdentity(ctx, replay.CallstackToCallpath(mockStableCallstack), replay.FuncToCallsite(fn.RuntimeFunc()))

			// on execution, the fn sees the identity the evaluator computed for it
			seen, err := evaluateWithCallstack(ctx, fn, struct{}{}, mockStableCallstack)
			assert.NoError(t, err)
			assert.Equal(t, expected, seen)

			// outside of the fn, the identity is not on the context
			testutil.PanicsWithErrorIs(t, moment.ErrNoMomentBeingEvaluated, func() {
				moment.CurrentIdentity(ctx)
			})
			return "success", nil
		}, nil)
		assert.NoError(t, err)
		assert.Equal(t, "success", result)
	})

	t.Run("does not memoize error", func(t *testing.T) {
		result, err := replay.Execute(runningFlowCtx(t), func(ctx context.Context, args any) (any, error) {
			f := execution.MustFromContext(ctx)
			ctx, _ = f.StartNewReplay(ctx)

			expectedError := errors.New("expectedError")
			callCount := 0
			fn := moment.NewFn(func(ctx context.Context, _ struct{}) (*struct{}, error) {
				callCount++
				return nil, expectedError
			})

			_, err := evaluateWithCallstack(ctx, fn, struct{}{}, mockStableCallstack)
			assert.Error(t, err)
			assert.ErrorIs(t, err, expectedError)
			assert.Equal(t, 0, sequence.GetIndex(ctx))
			assert.Equal(t, 1, callCount)

			ctx = sequence.With(ctx, execution.DefaultReplayFlags) // rewind
			_, err = evaluateWithCallstack(ctx, fn, struct{}{}, mockStableCallstack)
			assert.Error(t, err)
			assert.ErrorIs(t, err, expectedError)
			assert.Equal(t, 0, sequence.GetIndex(ctx))
			assert.Equal(t, 2, callCount)
			return "success", nil
		}, nil)
		assert.NoError(t, err)
		assert.Equal(t, "success", result)
	})

	t.Run("impure flow detection", func(t *testing.T) {
		t.Run("panics if the moment fn changes when it not allowed to", func(t *testing.T) {
			// the fn is part of the identity, so a different fn at the same callpath is a new branch
			result, err := replay.Execute(runningFlowCtx(t), func(ctx context.Context, args any) (any, error) {
				f := execution.MustFromContext(ctx)
				ctx, _ = f.StartNewReplay(ctx)

				fn1 := func(ctx context.Context, _ struct{}) (*struct{}, error) {
					return nil, nil
				}
				fn2 := func(ctx context.Context, _ struct{}) (*struct{}, error) {
					return nil, nil
				}

				// first eval as the code declared fn1 as this moment's fn
				_, err := evaluateWithCallstack(ctx, moment.NewFn(fn1), struct{}{}, mockStableCallstack)
				assert.NoError(t, err)

				// restart and eval as if the code re-declared fn2 as this moment's fn
				ctx = sequence.With(ctx, execution.DefaultReplayFlags) // rewind
				testutil.PanicsWithErrorIs(t, ErrUnexpectedBranchTaken, func() {
					evaluateWithCallstack(ctx, moment.NewFn(fn2), struct{}{}, mockStableCallstack)
				})
				assert.Equal(t, 0, sequence.GetIndex(ctx))
				return "success", nil
			}, nil)
			assert.NoError(t, err)
			assert.Equal(t, "success", result)
		})
		t.Run("panics if a new branch is taken when it not allowed to", func(t *testing.T) {
			result, err := replay.Execute(runningFlowCtx(t), func(ctx context.Context, args any) (any, error) {
				f := execution.MustFromContext(ctx)
				ctx, _ = f.StartNewReplay(ctx)

				fn1 := moment.NewFn(func(ctx context.Context, _ struct{}) (*struct{}, error) {
					return nil, nil
				})
				branch1 := []runtime.Frame{{File: "code.go", Line: 1}}
				fn2 := moment.NewFn(func(ctx context.Context, _ struct{}) (*struct{}, error) {
					return nil, nil
				})
				branch2 := []runtime.Frame{{File: "code.go", Line: 2}}

				// first eval as if we took branch 1, which uses fn1
				_, err := evaluateWithCallstack(ctx, fn1, struct{}{}, branch1)
				assert.NoError(t, err)

				// restart and eval as if we took branch 2, which uses fn2
				ctx = sequence.With(ctx, execution.DefaultReplayFlags) // rewind
				testutil.PanicsWithErrorIs(t, ErrUnexpectedBranchTaken, func() {
					evaluateWithCallstack(ctx, fn2, struct{}{}, branch2)
				})
				assert.Equal(t, 0, sequence.GetIndex(ctx))
				return "success", nil
			}, nil)
			assert.NoError(t, err)
			assert.Equal(t, "success", result)
		})
	})

	t.Run("wraps error with label", func(t *testing.T) {
		label := "testLabel"
		testErr := errors.New("expected error")
		result, err := replay.Execute(runningFlowCtx(t), func(ctx context.Context, args any) (any, error) {
			f := execution.MustFromContext(ctx)
			ctx, _ = f.StartNewReplay(ctx)
			_, err := evaluateWithCallstack(ctx, moment.NewFn(func(ctx context.Context, _ struct{}) (*struct{}, error) {
				return nil, testErr
			}, ftype.WithLabel(label)), struct{}{}, mockStableCallstack)
			assert.ErrorIs(t, err, ErrEvalFailed)
			assert.ErrorIs(t, err, testErr)
			assert.ErrorContains(t, err, label)
			return "success", nil
		}, nil)
		assert.NoError(t, err)
		assert.Equal(t, "success", result)
	})

	t.Run("panics if evaluated outside of a flow function", func(t *testing.T) {
		assert.PanicsWithError(t, ftrerrors.InconsistentStateError(ErrEvaledOutsideOfAFlowFunction).Error(), func() {
			Evaluate(runningFlowCtx(t), moment.NewFn(func(ctx context.Context, _ struct{}) (any, error) {
				return nil, nil
			}), struct{}{})
		})
	})

	t.Run("panics if evaluated from inside another step", func(t *testing.T) {
		// A step's fn receives a flow context, so a nested Step would evaluate at the outer step's
		// index and record ahead of it, leaving a call order that no later replay can follow.
		innerRan := false
		c := executiontype.NewInMemoryContainer()
		exec := execution.NewFlowExecutionWithContainer(containertest.NewStrict(c))
		stop, ok := exec.TryStartRun()
		if !ok {
			t.Fatalf("flow execution is already running")
		}
		t.Cleanup(stop)
		_, err := replay.Execute(execution.WithFlow(t.Context(), exec), func(ctx context.Context, args any) (any, error) {
			ctx, _ = exec.StartNewReplay(ctx)
			testutil.PanicsWithErrorIs(t, ErrNestedStep, func() {
				Evaluate(ctx, moment.NewFn(func(ctx context.Context, _ struct{}) (string, error) {
					Evaluate(ctx, moment.NewFn(func(ctx context.Context, _ struct{}) (string, error) {
						innerRan = true
						return "", nil
					}), struct{}{})
					return "", nil
				}), struct{}{})
			})
			return "success", nil
		}, nil)
		assert.NoError(t, err)
		assert.False(t, innerRan)
		assert.Equal(t, 0, c.CallOrderLength(), "nothing is recorded for a step that panicked")
	})

	t.Run("terminates without executing if the replay is cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(runningFlowCtx(t))
		executed := false
		terminatedWith := replayTerminatedWith(t, ctx, func(ctx context.Context) {
			ctx, _ = execution.MustFromContext(ctx).StartNewReplay(ctx)
			cancel() // the outer context: the replay is cancelled through it
			Evaluate(ctx, moment.NewFn(func(ctx context.Context, _ struct{}) (any, error) {
				executed = true
				return nil, nil
			}), struct{}{})
		})
		assert.ErrorIs(t, terminatedWith, ErrReplayTerminated)
		assert.False(t, executed)
	})

	t.Run("does not terminate for a context the flow derived and cancelled itself", func(t *testing.T) {
		result, err := replay.Execute(runningFlowCtx(t), func(ctx context.Context, args any) (any, error) {
			ctx, _ = execution.MustFromContext(ctx).StartNewReplay(ctx)
			derived, cancel := context.WithCancel(ctx)
			cancel()

			_, err := Evaluate(derived, moment.NewFn(func(ctx context.Context, _ struct{}) (string, error) {
				return "", ctx.Err()
			}), struct{}{})
			assert.ErrorIs(t, err, ErrEvalFailed)
			assert.ErrorIs(t, err, context.Canceled)
			return "success", nil
		}, nil)
		assert.NoError(t, err)
		assert.Equal(t, "success", result)
	})

	t.Run("terminates before evaluating if the replay was restarted", func(t *testing.T) {
		executed := false
		var indexAfter int
		terminatedWith := replayTerminatedWith(t, runningFlowCtx(t), func(ctx context.Context) {
			f := execution.MustFromContext(ctx)
			ctx, _ = f.StartNewReplay(ctx)
			f.WriteBehind(ctx, "restarted", nil)
			defer func() { indexAfter = sequence.GetIndex(ctx) }()

			Evaluate(ctx, moment.NewFn(func(ctx context.Context, _ struct{}) (string, error) {
				executed = true
				return "", nil
			}), struct{}{})
		})
		assert.ErrorIs(t, terminatedWith, ErrReplayTerminated)
		assert.False(t, executed)
		assert.Equal(t, 0, indexAfter)
	})

	t.Run("terminates instead of returning the fn's ctx error, if the replay was cancelled during the step", func(t *testing.T) {
		terminatedWith := replayTerminatedWith(t, runningFlowCtx(t), func(ctx context.Context) {
			f := execution.MustFromContext(ctx)
			ctx, _ = f.StartNewReplay(ctx)

			Evaluate(ctx, moment.NewFn(func(ctx context.Context, _ struct{}) (string, error) {
				f.WriteBehind(ctx, "restarted-from-inside-the-step", nil)
				return "", ctx.Err()
			}), struct{}{})
		})
		assert.ErrorIs(t, terminatedWith, ErrReplayTerminated)
	})

	t.Run("returns the fn's result if the replay was cancelled during the step but the fn did not observe it", func(t *testing.T) {
		result, err := replay.Execute(runningFlowCtx(t), func(ctx context.Context, args any) (any, error) {
			f := execution.MustFromContext(ctx)
			ctx, _ = f.StartNewReplay(ctx)

			result, err := Evaluate(ctx, moment.NewFn(func(ctx context.Context, _ struct{}) (string, error) {
				f.WriteBehind(ctx, "restarted-from-inside-the-step", nil)
				return "result", nil
			}), struct{}{})
			// real work: recorded and returned, the replay terminates at its next step instead
			assert.NoError(t, err)
			assert.Equal(t, "result", result)
			assert.Equal(t, 1, sequence.GetIndex(ctx))
			return "success", nil
		}, nil)
		assert.NoError(t, err)
		assert.Equal(t, "success", result)
	})

	t.Run("immediately returns with the injected error if there is one", func(t *testing.T) {
		expectedError := errors.New("expected error")
		ctx := testutil.WithInjectedError(runningFlowCtx(t), testutil.InjectedErrorLevelEvaluate, expectedError)
		_, err := Evaluate(ctx, moment.NewFn(func(ctx context.Context, _ struct{}) (any, error) {
			return nil, expectedError
		}), struct{}{})
		assert.ErrorIs(t, err, expectedError)
	})
}
