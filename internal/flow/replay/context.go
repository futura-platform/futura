package replay

import "context"

type replayKeyType struct{}

var replayKey = replayKeyType{}

type replayContext struct {
	context.Context
	cancel context.CancelCauseFunc
}

func With(ctx context.Context) (context.Context, context.CancelCauseFunc) {
	ctx, cancel := context.WithCancelCause(ctx)
	return context.WithValue(ctx, replayKey, replayContext{Context: ctx, cancel: cancel}), cancel
}

func Has(ctx context.Context) bool {
	_, ok := ctx.Value(replayKey).(replayContext)
	return ok
}

func Cancel(ctx context.Context, cause error) {
	replay, ok := ctx.Value(replayKey).(replayContext)
	if !ok {
		panic("cancel replay not found")
	}
	replay.cancel(cause)
}

// Err reports whether the replay that ctx belongs to has been cancelled.
// A context derived from the replay by the flow may be done while the replay itself is not.
func Err(ctx context.Context) error {
	replay, ok := ctx.Value(replayKey).(replayContext)
	if !ok {
		panic("replay not found")
	}
	return replay.Err()
}

// Cause returns why the replay that ctx belongs to was cancelled, or nil if it was not.
func Cause(ctx context.Context) error {
	replay, ok := ctx.Value(replayKey).(replayContext)
	if !ok {
		panic("replay not found")
	}
	return context.Cause(replay)
}

// Same reports whether a and b belong to the same replay.
func Same(a, b context.Context) bool {
	ra, ok := a.Value(replayKey).(replayContext)
	if !ok {
		panic("replay not found")
	}
	rb, ok := b.Value(replayKey).(replayContext)
	if !ok {
		panic("replay not found")
	}
	return ra.Context == rb.Context
}
