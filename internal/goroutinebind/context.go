package goroutinebind

import (
	"context"
	"errors"
	"fmt"

	"github.com/petermattis/goid"
)

type contextKey string

const (
	contextKeyGoroutineID contextKey = "bound_goroutine_id"
)

func BindGoroutine(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextKeyGoroutineID, goid.Get())
}

var (
	ErrNotBound = errors.New("context not bound to a goroutine")
)

type ErrWrongGoroutine struct {
	BoundGoroutineID    int64
	ObservedGoroutineID int64
}

func (e ErrWrongGoroutine) Error() string {
	return fmt.Sprintf("context bound to a different goroutine: %d != %d", e.BoundGoroutineID, e.ObservedGoroutineID)
}

func AssertBoundGoroutine(ctx context.Context) error {
	goroutineID, ok := ctx.Value(contextKeyGoroutineID).(int64)
	if !ok {
		return ErrNotBound
	} else if goroutineID != goid.Get() {
		return ErrWrongGoroutine{
			BoundGoroutineID:    goroutineID,
			ObservedGoroutineID: goid.Get(),
		}
	}
	return nil
}
