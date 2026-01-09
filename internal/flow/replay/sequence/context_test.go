package sequence_test

import (
	"context"
	"testing"

	"github.com/futura-platform/futura/internal/flow/replay/sequence"
	"github.com/stretchr/testify/assert"
)

func TestSequenceContext(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		ctx := sequence.With(t.Context())
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
}
