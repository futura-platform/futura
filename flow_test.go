package futura_test

import (
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/futura-platform/futura"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/fcontext"
	"github.com/futura-platform/futura/internal/flow/moment"
	"github.com/stretchr/testify/assert"
)

func TestFlow(t *testing.T) {
	t.Run("do not call Flow from within a flow", func(t *testing.T) {
		ctx := fcontext.WithFlow(context.Background(), nil)
		_, err := futura.Flow(ctx, func(b futura.FlowBuilder, _ *any) (string, error) {
			futura.Flow(b, func(b futura.FlowBuilder, _ *any) (string, error) {
				return "never reached 1", nil
			}, nil)
			return "never reached 2", nil
		}, nil)
		assert.ErrorIs(t, err, futura.ErrTopLevelFlowConflict)
	})
	t.Run("Flow recovers from panics", func(t *testing.T) {
		var expectedErr = errors.New("expected panic")
		_, err := futura.Flow(context.Background(), func(b futura.FlowBuilder, _ *any) (string, error) {
			panic(expectedErr)
		}, nil)
		assert.ErrorIs(t, err, futura.ErrFlowPanic)
		assert.ErrorIs(t, err, expectedErr)
	})
	t.Run("Flow recovers from panics with non-error values", func(t *testing.T) {
		_, err := futura.Flow(context.Background(), func(b futura.FlowBuilder, _ *any) (string, error) {
			panic("not an error type")
		}, nil)
		assert.ErrorIs(t, err, futura.ErrFlowPanic)
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
		r, err := futura.Flow(context.Background(), func(b futura.FlowBuilder, _ *any) (string, error) {
			var vfn futura.ComparableMoment[*any, string]
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
		_, err := checkMultipleMomentFunctions(t, nil, nil)
		_, file, _, _ := runtime.Caller(0)
		assert.ErrorIs(t, err, ftrerrors.ErrInconsistentState)
		assert.ErrorIs(t, err, moment.MomentFnChangeError{
			Index:          0,
			Identity:       moment.NewIdentity(context.Background(), []moment.Callsite{{File: file, Line: 65}}),
			OldMomentFnRef: moment.NewFn(fn1),
			NewMomentFnRef: moment.NewFn(fn2),
		})
	})
	t.Run("A single keyed moment identity should be able to be used with multiple moment functions", func(t *testing.T) {
		r, err := checkMultipleMomentFunctions(t, func(b futura.FlowBuilder) futura.FlowBuilder {
			return b.WithKey(1)
		}, func(b futura.FlowBuilder) futura.FlowBuilder {
			return b.WithKey(2)
		})
		assert.NoError(t, err)
		assert.Equal(t, "fn2", r)
	})
	t.Run("A single keyed moment identity should be able to be used with a single moment function, and have memoization keyed by the identity key", func(t *testing.T) {
		expectedExecCount := 10
		execCount := 0
		fn := func(ctx context.Context, args any) error {
			execCount++
			return nil
		}
		_, err := futura.Flow(context.Background(), func(b futura.FlowBuilder, _ *any) (string, error) {
			for i := range expectedExecCount {
				b = b.WithKey(i)
				err := futura.Effect(b, fn, nil)
				assert.NoError(t, err)
			}
			return "", nil
		}, nil)
		assert.NoError(t, err)
		assert.Equal(t, expectedExecCount, execCount)
	})
}
