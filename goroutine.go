package futura

import (
	"context"
	"errors"
	"fmt"

	"github.com/futura-platform/futura/internal/goroutinebind"
	"github.com/futura-platform/futura/internal/step"
	"github.com/petermattis/goid"
)

var (
	ErrGoroutineAlreadyBound = errors.New("goroutine already bound")
)

// BindToGoroutine binds the current goroutine to the context.
// This allows for step implementations to use goroutines safely.
// If all goroutines have not exited by the time the main goroutine finishes calling the step, the step will fail.
func BindToGoroutine(ctx context.Context) (context.Context, context.CancelFunc) {
	activeGoroutines := step.ActiveGoroutinesFrom(ctx)
	routineId := goid.Get()
	if !activeGoroutines.Add(routineId) {
		panic(fmt.Errorf("%w: %d", ErrGoroutineAlreadyBound, routineId))
	}
	return goroutinebind.BindGoroutine(ctx), func() {
		activeGoroutines.Remove(routineId)
	}
}
