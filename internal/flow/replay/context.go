package replay

import "context"

const replayKey = "replay"

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
