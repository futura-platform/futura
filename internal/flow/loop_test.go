package flow_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/flow"
	"github.com/futura-platform/futura/internal/flow/fcontext"
	"github.com/futura-platform/futura/internal/flow/moment"
	"github.com/futura-platform/futura/internal/step"
	"github.com/stretchr/testify/assert"
)

func TestLoopFlow(t *testing.T) {
	t.Run("Basic flow", func(t *testing.T) {
		ctx := fcontext.WithFlow(t.Context(), nil)
		rval, err := flow.Loop(ctx, func(ctx context.Context, _ *struct{}) (string, error) {
			return "test", nil
		}, &struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, "test", rval)
	})

	t.Run("Flow cancellation", func(t *testing.T) {
		ctx := fcontext.WithFlow(t.Context(), nil)
		_, err := flow.Loop(ctx, func(ctx context.Context, _ *struct{}) (string, error) {
			return "", ftype.ErrCancelFlow
		}, &struct{}{})
		assert.ErrorIs(t, err, ftype.ErrCancelFlow)
	})

	t.Run("Regular error handling", func(t *testing.T) {
		testErr := errors.New("test error")
		ctx := fcontext.WithFlow(t.Context(), nil)

		fnCallCount := 0
		rval, err := flow.Loop(
			ctx,
			func(ctx context.Context, _ *struct{}) (string, error) {
				fnCallCount++
				if fnCallCount >= 2 {
					return "result", nil
				}
				return "", testErr
			},
			&struct{}{},
		)
		assert.Equal(t, 2, fnCallCount)
		assert.Equal(t, "result", rval)
		assert.NoError(t, err)
	})

	t.Run("Context error handling", func(t *testing.T) {
		ctx := fcontext.WithFlow(t.Context(), nil)
		ctx, cancel := context.WithCancel(ctx)
		_, err := flow.Loop(ctx, func(ctx context.Context, _ *struct{}) (string, error) {
			cancel()
			return "", errors.New("unrelated forever error")
		}, &struct{}{})
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("The loop should replay if the replay context was cancelled, even if the callable flow returns without an error", func(t *testing.T) {
		ctx := fcontext.WithFlow(t.Context(), nil)
		f := fcontext.MustFromContext(ctx)
		replays := 0
		rval, err := flow.Loop(ctx, func(ctx context.Context, _ *struct{}) (string, error) {
			if replays == 0 {
				f.RestartCurrentReplay(ctx, errors.New("replay cancelled"))
			}
			replays++
			return fmt.Sprintf("success on replay %d", replays), nil
		}, &struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, "success on replay 2", rval)
	})

	t.Run("Evict cached moments when they are skipped", func(t *testing.T) {
		ctx := fcontext.WithFlow(t.Context(), nil)

		replays := 0
		fn1 := moment.NewFn(func(ctx context.Context, _ struct{}) (string, error) {
			return fmt.Sprintf("fn1 on replay %d", replays), nil
		}, ftype.WithLabel("fn1"))

		fn2 := moment.NewFn(func(ctx context.Context, _ struct{}) (string, error) {
			if replays < 3 {
				return "", errors.New("test error")
			}
			return fmt.Sprintf("fn2 on replay %d", replays), nil
		}, ftype.WithLabel("fn2"))

		f := fcontext.MustFromContext(ctx)
		r, err := flow.Loop(ctx, func(ctx context.Context, _ struct{}) (r string, err error) {
			f.SetReplayFlags(func(flags *fcontext.ReplayFlags) {
				// allow the moment order to change, since we're testing that the moment is evicted.
				flags.PanicOnMomentOrderChange = false
			})
			replays++
			r1 := "didnteval"
			if replays != 2 {
				r1, _, err = step.Evaluate(ctx, fn1, struct{}{})
				if err != nil {
					return "", err
				}
			}
			r2, _, err := step.Evaluate(ctx, fn2, struct{}{})
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%s, %s", r1, r2), nil
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 3, replays)
		assert.Equal(t, "fn1 on replay 3, fn2 on replay 3", r)
	})

	t.Run("End to end flow with steps", func(t *testing.T) {
		errCount := 0
		expectedErr := errors.New("test error")
		ctx := fcontext.WithFlow(t.Context(), []ftype.FlowLoopOption{
			ftype.WithOnStepError(func(err error) (continueExecution bool) {
				assert.ErrorIs(t, err, expectedErr)
				errCount++
				return true
			}),
		})

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

		rval, err := flow.Loop(ctx, func(ctx context.Context, _ *any) (string, error) {
			// todo: make this include a conditional branch to cover moment eviction.
			r1, _, err := step.Evaluate(ctx, failsTwice, nil)
			if err != nil {
				return "", err
			}

			r2, _, err := step.Evaluate(ctx, fn2, nil)
			if err != nil {
				return "", err
			}
			return r1 + r2, nil
		}, nil)
		assert.NoError(t, err)
		assert.Equal(t, "fn1fn2", rval)
		assert.Equal(t, 2, errCount)
		assert.Equal(t, 3, fn1Calls)
	})

	t.Run("Applies context wrappers in order", func(t *testing.T) {
		const collidingKey = "collidingKey"
		wrapper1Value := "wrapper1"
		wrapper2Value := "wrapper2"
		ctx := fcontext.WithFlow(t.Context(), []ftype.FlowLoopOption{
			func(flo *ftype.FlowLoopOptions) {
				flo.ContextWrappers = append(flo.ContextWrappers, func(ctx context.Context) context.Context {
					return context.WithValue(ctx, collidingKey, wrapper1Value)
				})
			},
			func(flo *ftype.FlowLoopOptions) {
				flo.ContextWrappers = append(flo.ContextWrappers, func(ctx context.Context) context.Context {
					return context.WithValue(ctx, collidingKey, wrapper2Value)
				})
			},
		})
		flow.Loop(ctx, func(ctx context.Context, _ *struct{}) (string, error) {
			assert.Equal(t, wrapper2Value, ctx.Value(collidingKey))
			return "test", nil
		}, &struct{}{})
	})
}
