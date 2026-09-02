package replay

import "context"

const cancelReplayKey = "cancel_replay"

func With(ctx context.Context) (context.Context, context.CancelCauseFunc) {
	ctx, cancel := context.WithCancelCause(ctx)
	return context.WithValue(ctx, cancelReplayKey, cancel), cancel
}

func Has(ctx context.Context) bool {
	_, ok := ctx.Value(cancelReplayKey).(context.CancelCauseFunc)
	return ok
}

func Cancel(ctx context.Context, cause error) {
	cancel, ok := ctx.Value(cancelReplayKey).(context.CancelCauseFunc)
	if !ok {
		panic("cancel replay not found")
	}
	cancel(cause)
}
