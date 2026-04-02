package sequence_test

import (
	"context"
	"testing"

	"github.com/futura-platform/futura/internal/flow/replay"
	"github.com/futura-platform/futura/internal/flow/replay/sequence"
	"github.com/stretchr/testify/assert"
)

func TestSequenceContext(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		ctx := sequence.With(t.Context(), replay.Flags{})
		assert.Equal(t, 0, sequence.GetIndex(ctx))
		for i := range 10 {
			sequence.Advance(ctx)
			assert.Equal(t, i+1, sequence.GetIndex(ctx))
		}
	})
	t.Run("panic on undefined key", func(t *testing.T) {
		ctx := context.Background()
		assert.Panics(t, func() { sequence.GetIndex(ctx) })
	})
	t.Run("flags", func(t *testing.T) {
		ctx := sequence.With(t.Context(), replay.Flags{
			PanicOnMomentOrderChange: true,
		})
		assert.True(t, sequence.GetFlags(ctx).PanicOnMomentOrderChange)
	})
	t.Run("run deferred with none", func(t *testing.T) {
		ctx := sequence.With(t.Context(), replay.Flags{})
		assert.NotPanics(t, func() {
			sequence.RunDeferred(ctx)
		})
	})
	t.Run("deferred functions run in LIFO order", func(t *testing.T) {
		ctx := sequence.With(t.Context(), replay.Flags{})
		var calls []int

		sequence.Defer(ctx, func() { calls = append(calls, 1) })
		sequence.Defer(ctx, func() { calls = append(calls, 2) })
		sequence.Defer(ctx, func() { calls = append(calls, 3) })

		sequence.RunDeferred(ctx)

		assert.Equal(t, []int{3, 2, 1}, calls)
	})
}
