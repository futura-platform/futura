package flow

import (
	"context"
	"errors"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/flow/replay"
)

type CallableFlow[A, T any] func(ctx context.Context, args A) (T, error)

// Loop implements the core logic of the flow. It is responsible for:
// - executing the flow fn
// - handling errors
// - rewinding the sequence
func Loop[A, T any](ctx context.Context, callableFlow CallableFlow[A, T], args A, opts ...ftype.FlowLoopOption) (T, error) {
	f := execution.MustFromContext(ctx)
	for _, opt := range opts {
		ctx = opt(ctx)
	}

	for ; ; func() {
		f.SetReplayFlags(func(flags *execution.ReplayFlags) {
			flags.PanicOnMomentOrderChange = true
		})
		f.EvictUnseenCachedStates(ctx)
	}() {
	restartReplay:
		replayCtx := f.StartNewReplay(ctx)
		result, err := replay.Execute(replayCtx, callableFlow, args)
		replay.Cancel(replayCtx, nil)
		if ctx.Err() != nil {
			// if the context is done, comply by returning immediately
			return result, ctx.Err()
		} else if errors.Is(context.Cause(replayCtx), execution.ErrRestartReplay) {
			// special case to restart the replay without performing the default end of replay behavior.
			goto restartReplay
		} else if err == nil {
			return result, nil
		} else if errors.Is(err, ftype.ErrCancelFlow) {
			// special case to immedieately return the error from the loop.
			return result, err
		}
	}
}
