package step

import (
	"context"
	"errors"
	"testing"

	"github.com/futura-platform/futura/internal/flow/replay"
	"github.com/stretchr/testify/assert"
)

func TestActiveGoroutines(t *testing.T) {
	termination := func() error {
		ctx, _ := replay.With(context.Background())
		return ReplayTerminatedError{Replay: ctx}
	}
	t.Run("a crash beats a termination from another goroutine, whichever started first", func(t *testing.T) {
		for _, terminationFirst := range []bool{true, false} {
			g, _ := withActiveGoroutines(t.Context())
			first, _ := g.Start()
			second, _ := g.Start()
			if terminationFirst {
				first(termination())
				second(errors.New("real boom"))
			} else {
				first(errors.New("real boom"))
				second(termination())
			}
			err := g.End()
			assert.ErrorContains(t, err, "real boom")
			assert.NotErrorIs(t, err, ErrReplayTerminated)
		}
	})
	t.Run("a termination alone is returned as it is", func(t *testing.T) {
		g, _ := withActiveGoroutines(t.Context())
		done, _ := g.Start()
		done(termination())
		_, ok := AsReplayTerminated(g.End())
		assert.True(t, ok)
	})
	t.Run("a panic cancels the step's context with itself", func(t *testing.T) {
		g, ctx := withActiveGoroutines(t.Context())
		done, _ := g.Start()
		boom := errors.New("real boom")
		done(boom)
		assert.ErrorIs(t, context.Cause(ctx), boom)
	})
	t.Run("a leak is reported with the panics of those that ended", func(t *testing.T) {
		g, _ := withActiveGoroutines(t.Context())
		g.Start()
		done, _ := g.Start()
		done(errors.New("real boom"))
		err := g.End()
		assert.ErrorIs(t, err, ErrGoroutinesNotExited)
		assert.ErrorContains(t, err, "real boom")
	})
}
