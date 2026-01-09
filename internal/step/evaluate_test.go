package step

import (
	"context"
	"errors"
	"reflect"
	"runtime"

	"testing"

	"github.com/futura-platform/futura/ftype"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/flow/moment"
	"github.com/futura-platform/futura/internal/flow/replay"
	"github.com/futura-platform/futura/internal/flow/replay/sequence"
	"github.com/futura-platform/futura/internal/utils/testutil"

	"github.com/stretchr/testify/assert"
)

var mockStableCallstack = []runtime.Frame{{
	File: "mock.go",
	Line: 1,
}}

func TestStep(t *testing.T) {
	t.Run("memoize result for identical inputs", func(t *testing.T) {
		replay.Execute(execution.WithFlow(t.Context(), execution.NewFlowExecution()), func(ctx context.Context, args any) (any, error) {
			f := execution.MustFromContext(ctx)
			ctx = f.StartNewReplay(ctx)

			expectedResult := "expectedResult"
			callCount := 0
			fn := moment.NewFn(func(ctx context.Context, _ struct{}) (string, error) {
				callCount++
				return expectedResult, nil
			})

			result1, _, err := evaluateWithCallstack(ctx, fn, struct{}{}, mockStableCallstack)
			assert.NoError(t, err)
			assert.Equal(t, expectedResult, result1)
			assert.Equal(t, 1, sequence.GetIndex(ctx))
			assert.Equal(t, 1, callCount)

			ctx = sequence.With(ctx) // rewind
			result2, _, err := evaluateWithCallstack(ctx, fn, struct{}{}, mockStableCallstack)
			assert.NoError(t, err)
			assert.Equal(t, result1, result2)
			assert.Equal(t, 1, sequence.GetIndex(ctx))
			assert.Equal(t, 1, callCount)
			return nil, nil
		}, nil)
	})

	t.Run("does not memoize error", func(t *testing.T) {
		replay.Execute(execution.WithFlow(t.Context(), execution.NewFlowExecution()), func(ctx context.Context, args any) (any, error) {
			f := execution.MustFromContext(ctx)
			ctx = f.StartNewReplay(ctx)

			expectedError := errors.New("expectedError")
			callCount := 0
			fn := moment.NewFn(func(ctx context.Context, _ struct{}) (any, error) {
				callCount++
				return nil, expectedError
			})

			_, _, err := evaluateWithCallstack(ctx, fn, struct{}{}, mockStableCallstack)
			assert.Error(t, err)
			assert.ErrorIs(t, err, expectedError)
			assert.Equal(t, 0, sequence.GetIndex(ctx))
			assert.Equal(t, 1, callCount)

			ctx = sequence.With(ctx) // rewind
			_, _, err = evaluateWithCallstack(ctx, fn, struct{}{}, mockStableCallstack)
			assert.Error(t, err)
			assert.ErrorIs(t, err, expectedError)
			assert.Equal(t, 0, sequence.GetIndex(ctx))
			assert.Equal(t, 2, callCount)
			return nil, nil
		}, nil)
	})

	t.Run("manual memo invalidation", func(t *testing.T) {
		replay.Execute(execution.WithFlow(t.Context(), execution.NewFlowExecution()), func(ctx context.Context, args any) (any, error) {
			f := execution.MustFromContext(ctx)
			ctx = f.StartNewReplay(ctx)

			calls := 0
			fn := moment.NewFn(func(ctx context.Context, _ struct{}) (int, error) {
				calls++
				return calls, nil
			})

			result1, invalidate, err := evaluateWithCallstack(ctx, fn, struct{}{}, mockStableCallstack)
			assert.NoError(t, err)
			assert.Equal(t, 1, result1)

			invalidate()
			ctx = sequence.With(ctx) // rewind
			result2, _, err := evaluateWithCallstack(ctx, fn, struct{}{}, mockStableCallstack)
			assert.NoError(t, err)
			assert.Equal(t, 2, result2)
			return nil, nil
		}, nil)
	})

	t.Run("impure flow detection", func(t *testing.T) {
		t.Run("panics if the moment fn changes when it not allowed to", func(t *testing.T) {
			replay.Execute(execution.WithFlow(t.Context(), execution.NewFlowExecution()), func(ctx context.Context, args any) (any, error) {
				f := execution.MustFromContext(ctx)
				f.SetReplayFlags(func(flags *execution.ReplayFlags) {
					flags.PanicOnMomentOrderChange = true
				})
				ctx = f.StartNewReplay(ctx)

				fn1 := func(ctx context.Context, _ struct{}) (any, error) {
					return nil, nil
				}
				fn2 := func(ctx context.Context, _ struct{}) (any, error) {
					return nil, nil
				}

				// first eval as the code declared fn1 as this moment's fn
				_, _, err := evaluateWithCallstack(ctx, moment.NewFn(fn1), struct{}{}, mockStableCallstack)
				assert.NoError(t, err)

				// restart and eval as if the code re-declared fn2 as this moment's fn
				ctx = sequence.With(ctx) // rewind
				expectedError := ftrerrors.InconsistentStateError(moment.MomentFnChangeError{
					Index:           0,
					Identity:        moment.NewIdentity(ctx, replay.CallstackToCallpath(mockStableCallstack)),
					OldMomentFnName: runtime.FuncForPC(reflect.ValueOf(fn1).Pointer()).Name(),
					NewMomentFnName: runtime.FuncForPC(reflect.ValueOf(fn2).Pointer()).Name(),
				})
				assert.PanicsWithError(t, expectedError.Error(), func() {
					evaluateWithCallstack(ctx, moment.NewFn(fn2), struct{}{}, mockStableCallstack)
				})
				assert.Equal(t, 0, sequence.GetIndex(ctx))
				return nil, nil
			}, nil)
		})
		t.Run("panics if a new branch is taken when it not allowed to", func(t *testing.T) {
			replay.Execute(execution.WithFlow(t.Context(), execution.NewFlowExecution()), func(ctx context.Context, args any) (any, error) {
				f := execution.MustFromContext(ctx)
				f.SetReplayFlags(func(flags *execution.ReplayFlags) {
					flags.PanicOnMomentOrderChange = true
				})
				ctx = f.StartNewReplay(ctx)

				fn1 := moment.NewFn(func(ctx context.Context, _ struct{}) (any, error) {
					return nil, nil
				})
				branch1 := []runtime.Frame{{File: "code.go", Line: 1}}
				fn2 := moment.NewFn(func(ctx context.Context, _ struct{}) (any, error) {
					return nil, nil
				})
				branch2 := []runtime.Frame{{File: "code.go", Line: 2}}

				// first eval as if we took branch 1, which uses fn1
				_, _, err := evaluateWithCallstack(ctx, fn1, struct{}{}, branch1)
				assert.NoError(t, err)

				// restart and eval as if we took branch 2, which uses fn2
				ctx = sequence.With(ctx) // rewind
				testutil.PanicsWithErrorIs(t, ErrUnexpectedBranchTaken, func() {
					evaluateWithCallstack(ctx, fn2, struct{}{}, branch2)
				})
				assert.Equal(t, 0, sequence.GetIndex(ctx))
				return nil, nil
			}, nil)
		})
	})

	t.Run("wraps error with label", func(t *testing.T) {
		label := "testLabel"
		testErr := errors.New("expected error")
		replay.Execute(execution.WithFlow(t.Context(), execution.NewFlowExecution()), func(ctx context.Context, args any) (any, error) {
			f := execution.MustFromContext(ctx)
			ctx = f.StartNewReplay(ctx)
			_, _, err := evaluateWithCallstack(ctx, moment.NewFn(func(ctx context.Context, _ struct{}) (any, error) {
				return nil, testErr
			}, ftype.WithLabel(label)), struct{}{}, mockStableCallstack)
			assert.ErrorIs(t, err, ErrEvalFailed)
			assert.ErrorIs(t, err, testErr)
			assert.ErrorContains(t, err, label)
			return nil, nil
		}, nil)
	})

	t.Run("panics if evaluated outside of a flow function", func(t *testing.T) {
		assert.PanicsWithError(t, ftrerrors.InconsistentStateError(ErrEvaledOutsideOfAFlowFunction).Error(), func() {
			Evaluate(execution.WithFlow(t.Context(), execution.NewFlowExecution()), moment.NewFn(func(ctx context.Context, _ struct{}) (any, error) {
				return nil, nil
			}), struct{}{})
		})
	})

	t.Run("immediately returns without executing if the context is done", func(t *testing.T) {
		ctx, cancel := context.WithCancel(execution.WithFlow(t.Context(), execution.NewFlowExecution()))
		cancel()
		didExecute := false
		replay.Execute(ctx, func(ctx context.Context, args any) (any, error) {
			_, _, err := Evaluate(ctx, moment.NewFn(func(ctx context.Context, _ struct{}) (any, error) {
				t.Fatal("should not have executed")
				return nil, nil
			}), struct{}{})
			assert.ErrorIs(t, err, context.Canceled)
			assert.False(t, didExecute)
			return nil, nil
		}, nil)
	})

	t.Run("immedietly returns with the injected error if there is one", func(t *testing.T) {
		expectedError := errors.New("expected error")
		ctx := testutil.WithInjectedError(execution.WithFlow(t.Context(), execution.NewFlowExecution()), testutil.InjectedErrorLevelEvaluate, expectedError)
		_, _, err := Evaluate(ctx, moment.NewFn(func(ctx context.Context, _ struct{}) (any, error) {
			return nil, expectedError
		}), struct{}{})
		assert.ErrorIs(t, err, expectedError)
	})
}
