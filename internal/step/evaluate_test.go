package step

import (
	"context"
	"errors"
	"testing"

	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/fcontext"
	"github.com/futura-platform/futura/internal/flow/moment"
	"github.com/futura-platform/futura/internal/flow/replay"
	"github.com/stretchr/testify/assert"
)

var mockStableCallpath = []moment.Callsite{{
	File: "mock.go",
	Line: 1,
}}

func TestStep(t *testing.T) {
	t.Run("memoize result for identical inputs", func(t *testing.T) {
		replay.Execute(fcontext.WithFlow(t.Context(), nil), func(ctx context.Context, args any) (any, error) {
			f := fcontext.MustFromContext(ctx)
			ctx, cancel := f.StartNewReplay(ctx)
			defer cancel(nil)

			expectedResult := "expectedResult"
			callCount := 0
			fn := moment.NewFn(func(ctx context.Context, _ struct{}) (string, error) {
				callCount++
				return expectedResult, nil
			})

			result1, _, err := evaluateWithCallsite(ctx, fn, struct{}{}, mockStableCallpath)
			assert.NoError(t, err)
			assert.Equal(t, expectedResult, result1)
			assert.Equal(t, 1, f.SequenceIndex())
			assert.Equal(t, 1, callCount)

			f.Rewind()
			result2, _, err := evaluateWithCallsite(ctx, fn, struct{}{}, mockStableCallpath)
			assert.NoError(t, err)
			assert.Equal(t, result1, result2)
			assert.Equal(t, 1, f.SequenceIndex())
			assert.Equal(t, 1, callCount)
			return nil, nil
		}, nil)
	})

	t.Run("does not memoize error", func(t *testing.T) {
		replay.Execute(fcontext.WithFlow(t.Context(), nil), func(ctx context.Context, args any) (any, error) {
			f := fcontext.MustFromContext(ctx)
			ctx, cancel := f.StartNewReplay(ctx)
			defer cancel(nil)

			expectedError := errors.New("expectedError")
			callCount := 0
			fn := moment.NewFn(func(ctx context.Context, _ struct{}) (any, error) {
				callCount++
				return nil, expectedError
			})

			_, _, err := evaluateWithCallsite(ctx, fn, struct{}{}, mockStableCallpath)
			assert.Error(t, err)
			assert.Equal(t, expectedError, err)
			assert.Equal(t, 0, f.SequenceIndex())
			assert.Equal(t, 1, callCount)

			f.Rewind()
			_, _, err = evaluateWithCallsite(ctx, fn, struct{}{}, mockStableCallpath)
			assert.Error(t, err)
			assert.Equal(t, expectedError, err)
			assert.Equal(t, 0, f.SequenceIndex())
			assert.Equal(t, 2, callCount)
			return nil, nil
		}, nil)
	})

	t.Run("manual memo invalidation", func(t *testing.T) {
		replay.Execute(fcontext.WithFlow(t.Context(), nil), func(ctx context.Context, args any) (any, error) {
			f := fcontext.MustFromContext(ctx)
			ctx, cancel := f.StartNewReplay(ctx)
			defer cancel(nil)

			calls := 0
			fn := moment.NewFn(func(ctx context.Context, _ struct{}) (int, error) {
				calls++
				return calls, nil
			})

			result1, invalidate, err := evaluateWithCallsite(ctx, fn, struct{}{}, mockStableCallpath)
			assert.NoError(t, err)
			assert.Equal(t, 1, result1)

			invalidate()
			f.Rewind()
			result2, _, err := evaluateWithCallsite(ctx, fn, struct{}{}, mockStableCallpath)
			assert.NoError(t, err)
			assert.Equal(t, 2, result2)
			return nil, nil
		}, nil)
	})

	t.Run("impure flow detection", func(t *testing.T) {
		replay.Execute(fcontext.WithFlow(t.Context(), nil), func(ctx context.Context, args any) (any, error) {
			f := fcontext.MustFromContext(ctx)
			f.SetReplayFlags(func(flags *fcontext.ReplayFlags) {
				flags.PanicOnMomentOrderChange = true
			})
			ctx, cancel := f.StartNewReplay(ctx)
			defer cancel(nil)

			fn1 := moment.NewFn(func(ctx context.Context, _ struct{}) (any, error) {
				return nil, nil
			})
			fn2 := moment.NewFn(func(ctx context.Context, _ struct{}) (any, error) {
				return nil, nil
			})

			_, _, err := evaluateWithCallsite(ctx, fn1, struct{}{}, mockStableCallpath)
			assert.NoError(t, err)

			f.Rewind()
			expectedError := ftrerrors.InconsistentStateError(moment.MomentFnChangeError{
				Index:          0,
				Callpath:       mockStableCallpath,
				OldMomentFnRef: fn1,
				NewMomentFnRef: fn2,
			})
			assert.PanicsWithError(t, expectedError.Error(), func() {
				evaluateWithCallsite(ctx, fn2, struct{}{}, mockStableCallpath)
			})
			assert.Equal(t, 0, f.SequenceIndex())
			return nil, nil
		}, nil)
	})

	t.Run("panics if evaluated outside of a flow function", func(t *testing.T) {
		assert.PanicsWithError(t, ftrerrors.InconsistentStateError(ErrEvaledOutsideOfAFlowFunction).Error(), func() {
			Evaluate(fcontext.WithFlow(t.Context(), nil), moment.NewFn(func(ctx context.Context, _ struct{}) (any, error) {
				return nil, nil
			}), struct{}{})
		})
	})

	t.Run("immediately returns without executing if the context is done", func(t *testing.T) {
		ctx, cancel := context.WithCancel(fcontext.WithFlow(context.Background(), nil))
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
}
