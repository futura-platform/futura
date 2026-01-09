package goroutinebind_test

import (
	"context"
	"sync"
	"testing"

	"github.com/futura-platform/futura/internal/goroutinebind"
	"github.com/petermattis/goid"
	"github.com/stretchr/testify/assert"
)

func TestBindGoroutine(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctx := context.Background()
		ctx = goroutinebind.BindGoroutine(ctx)
		assert.NoError(t, goroutinebind.AssertBoundGoroutine(ctx))
	})
	t.Run("error", func(t *testing.T) {
		t.Run("not bound", func(t *testing.T) {
			ctx := context.Background()
			assert.ErrorIs(t, goroutinebind.AssertBoundGoroutine(ctx), goroutinebind.ErrNotBound)
		})
		t.Run("bound to different goroutine", func(t *testing.T) {
			ctx := context.Background()
			ctx = goroutinebind.BindGoroutine(ctx)
			var wg sync.WaitGroup
			boundGoroutineID := goid.Get()
			wg.Go(func() {
				otherGoroutineID := goid.Get()
				assert.ErrorIs(t, goroutinebind.AssertBoundGoroutine(ctx), goroutinebind.ErrWrongGoroutine{
					BoundGoroutineID:    boundGoroutineID,
					ObservedGoroutineID: otherGoroutineID,
				})
			})
			wg.Wait()
		})
	})
}
