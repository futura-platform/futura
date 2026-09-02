package replay_test

import (
	"context"
	"errors"
	"testing"

	"github.com/futura-platform/futura/internal/flow/replay"
	"github.com/stretchr/testify/assert"
)

func TestContext(t *testing.T) {
	t.Run("With", func(t *testing.T) {
		ctx := context.Background()
		ctx, _ = replay.With(ctx)
		assert.True(t, replay.Has(ctx))
	})
	t.Run("Cancel", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			ctx := context.Background()
			ctx, _ = replay.With(ctx)
			expectedCause := errors.New("test")
			replay.Cancel(ctx, expectedCause)
			assert.ErrorIs(t, context.Cause(ctx), expectedCause)
		})
		t.Run("error", func(t *testing.T) {
			ctx := context.Background()
			assert.Panics(t, func() {
				replay.Cancel(ctx, errors.New("test"))
			})
		})
	})
}
