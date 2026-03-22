package moment_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"reflect"
	"runtime"
	"testing"

	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/moment"
	"github.com/stretchr/testify/assert"
)

func TestValidate(t *testing.T) {
	t.Run("valid case", func(t *testing.T) {
		fn := moment.NewFn(func(ctx context.Context, args int) (int, error) {
			return args, nil
		})
		moment1 := moment.NewMoment(fn, 1)
		assert.True(t, moment1.Validate(0, fn, 1, moment.Identity{}))
	})
	t.Run("invalid cases", func(t *testing.T) {
		t.Run("input changed", func(t *testing.T) {
			fn := moment.NewFn(func(ctx context.Context, args int) (int, error) {
				return args, nil
			})
			moment1 := moment.NewMoment(fn, 1)
			assert.False(t, moment1.Validate(0, fn, 2, moment.Identity{}))
		})
		t.Run("moment was explicitly invalidated, otherwise valid", func(t *testing.T) {
			fn := moment.NewFn(func(ctx context.Context, args int) (int, error) {
				return args, nil
			})
			moment1 := moment.NewMoment(fn, 1)
			assert.True(t, moment1.Validate(0, fn, 1, moment.Identity{}))

			moment1.Invalidate()
			assert.False(t, moment1.Validate(0, fn, 1, moment.Identity{}))

			t.Run("setting the output to a different value makes it valid again", func(t *testing.T) {
				moment1.SetValidOutput(2)
				assert.True(t, moment1.Validate(0, fn, 1, moment.Identity{}))
			})
		})
	})
	t.Run("fatal divergence case", func(t *testing.T) {
		fn1 := func(ctx context.Context, args int) (int, error) {
			return args, nil
		}
		fn2 := func(ctx context.Context, args int) (int, error) {
			return args, nil
		}
		moment1 := moment.NewMoment(moment.NewFn(fn1), 1)
		identity := moment.NewIdentity(t.Context(), moment.Callpath{{File: "placeholder"}})
		assert.PanicsWithError(t, ftrerrors.InconsistentStateError(moment.MomentFnChangeError{
			Index:           0,
			OldMomentFnName: runtime.FuncForPC(reflect.ValueOf(fn1).Pointer()).Name(),
			NewMomentFnName: runtime.FuncForPC(reflect.ValueOf(fn2).Pointer()).Name(),
			Identity:        identity,
		}).Error(), func() {
			moment1.Validate(0, moment.NewFn(fn2), 2, identity)
		})
	})
}

func TestOutput(t *testing.T) {
	fn := moment.NewFn(func(ctx context.Context, args int) (int, error) {
		return args, nil
	})
	moment1 := moment.NewMoment(fn, 1)
	output := rand.Int()
	moment1.SetValidOutput(output)
	assert.Equal(t, output, moment1.Output())
}

func TestMomentFnChangeError(t *testing.T) {
	fn1 := func(ctx context.Context, args int) (int, error) {
		return args, nil
	}
	fn2 := func(ctx context.Context, args int) (int, error) {
		return args, nil
	}
	identity := moment.NewIdentity(t.Context(), moment.Callpath{{File: "placeholder"}})
	fnChangeErr := moment.MomentFnChangeError{
		Index:           0,
		OldMomentFnName: runtime.FuncForPC(reflect.ValueOf(fn1).Pointer()).Name(),
		NewMomentFnName: runtime.FuncForPC(reflect.ValueOf(fn2).Pointer()).Name(),
		Identity:        identity,
	}
	assert.ErrorIs(t, ftrerrors.InconsistentStateError(fnChangeErr), fnChangeErr)
	assert.NotErrorIs(t, ftrerrors.InconsistentStateError(fnChangeErr), errors.New("some other error"))
}
