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
		assert.ErrorIs(t, err, expectedErr)
	})
	t.Run("A single callsite should only ever be used with a single moment function", func(t *testing.T) {
		fn1 := func(ctx context.Context, args *any) (string, error) {
			return "fn1", errors.New("expected error")
		}
		fn2 := func(ctx context.Context, args *any) (string, error) {
			return "fn2", nil
		}
		replayCount := 0
		_, err := futura.Flow(context.Background(), func(b futura.FlowBuilder, _ *any) (string, error) {
			var vfn futura.ComparableMoment[*any, string]
			if replayCount == 0 {
				vfn = fn1
			} else {
				vfn = fn2
			}
			replayCount++
			return futura.Step(b, vfn, nil)
		}, nil)
		_, file, _, _ := runtime.Caller(0)
		assert.ErrorIs(t, err, ftrerrors.ErrInconsistentState)
		assert.ErrorIs(t, err, moment.MomentFnChangeError{
			Index:          0,
			Identity:       moment.NewIdentity(context.Background(), []moment.Callsite{{File: file, Line: 50}}),
			OldMomentFnRef: moment.NewFn(fn1),
			NewMomentFnRef: moment.NewFn(fn2),
		})
		assert.Equal(t, 2, replayCount)
	})
}
