package fopt_test

import (
	"context"
	"testing"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/fopt"
	"github.com/futura-platform/futura/internal/step"
	"github.com/futura-platform/futura/internal/utils/containertest"
	"github.com/stretchr/testify/assert"
)

func TestWithCodeVersion(t *testing.T) {
	fnA := func(ctx context.Context, _ *any) (string, error) { return "A", nil }
	fnB := func(ctx context.Context, _ *any) (string, error) { return "B", nil }
	fnC := func(ctx context.Context, _ *any) (string, error) { return "C", nil }
	// body evaluates fn at one callsite, so swapping fn is a branch the recorded call order did not take
	body := func(fn futura.ComparableMomentFn[*any, string]) func(futura.FlowBuilder, *any) (string, error) {
		return func(b futura.FlowBuilder, _ *any) (string, error) {
			return futura.Step(b, fn, nil)
		}
	}

	t.Run("a new version relaxes the strictness of the next replay", func(t *testing.T) {
		f := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory())
		r, err := f.Execute(t.Context(), body(fnA), nil, fopt.WithCodeVersion("1"))
		assert.NoError(t, err)
		assert.Equal(t, "A", r)

		r, err = f.Execute(t.Context(), body(fnB), nil, fopt.WithCodeVersion("2"))
		assert.NoError(t, err)
		assert.Equal(t, "B", r)
	})

	t.Run("the same version keeps the next replay strict", func(t *testing.T) {
		f := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory())
		_, err := f.Execute(t.Context(), body(fnA), nil, fopt.WithCodeVersion("1"))
		assert.NoError(t, err)

		_, err = f.Execute(t.Context(), body(fnB), nil, fopt.WithCodeVersion("1"))
		assert.ErrorIs(t, err, step.ErrUnexpectedBranchTaken)
	})

	t.Run("the relaxation lasts until a replay settles", func(t *testing.T) {
		f := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory())
		_, err := f.Execute(t.Context(), body(fnA), nil, fopt.WithCodeVersion("1"))
		assert.NoError(t, err)
		_, err = f.Execute(t.Context(), body(fnB), nil, fopt.WithCodeVersion("2"))
		assert.NoError(t, err)

		_, err = f.Execute(t.Context(), body(fnC), nil, fopt.WithCodeVersion("2"))
		assert.ErrorIs(t, err, step.ErrUnexpectedBranchTaken)
	})

	t.Run("a container that never ran under a version is relaxed once", func(t *testing.T) {
		f := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory())
		_, err := f.Execute(t.Context(), body(fnA), nil)
		assert.NoError(t, err)

		r, err := f.Execute(t.Context(), body(fnB), nil, fopt.WithCodeVersion("1"))
		assert.NoError(t, err)
		assert.Equal(t, "B", r)
	})
}
