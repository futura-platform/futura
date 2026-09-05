package futura

import (
	"context"

	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/goroutinebind"
	"github.com/futura-platform/futura/internal/step"
)

// Goroutine is a goroutine of a step, started by Go.
type Goroutine struct {
	exited <-chan struct{}
}

// Wait blocks until the goroutine has exited. Waiting on a Goroutine that was never started returns at once.
func (g Goroutine) Wait() {
	if g.exited != nil {
		<-g.exited
	}
}

// Go runs fn on a goroutine of the step, and returns it for the step to Wait on. A step that returns
// under a running goroutine fails, and a panic of fn is a panic of the step; fn returns its errors to
// the step the way any Go code does. The context fn receives is bound to its goroutine.
func Go(ctx context.Context, fn func(ctx context.Context)) Goroutine {
	done, exited := step.ActiveGoroutinesFrom(ctx).Start()
	go func() {
		var panicked error
		defer func() { done(panicked) }()
		defer func() {
			if r := recover(); r != nil {
				panicked = ftrerrors.PanicError(r)
			}
		}()
		fn(goroutinebind.BindGoroutine(ctx))
	}()
	return Goroutine{exited: exited}
}
