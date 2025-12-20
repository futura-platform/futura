package flow

import (
	"context"
	"errors"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/flow/fcontext"
	"github.com/futura-platform/futura/internal/flow/replay"
)

// Loop implements the core logic of the flow. It is responsible for:
// - executing the flow fn
// - handling errors
// - rewinding the sequence
func Loop[A, T any](ctx context.Context, callableFlow func(ctx context.Context, args A) (T, error), args A) (T, error) {
	f := fcontext.MustFromContext(ctx)
	opts := &ftype.FlowLoopOptions{}
	for _, option := range f.Options() {
		option(opts)
	}

	for _, contextWrapper := range opts.ContextWrappers {
		ctx = contextWrapper(ctx)
	}

	for ; ; func() {
		f.SetReplayFlags(func(flags *fcontext.ReplayFlags) {
			flags.PanicOnMomentOrderChange = true
		})
		f.Rewind()
		f.EvictUnseenCachedStates(ctx)
	}() {
	restartReplay:
		replayCtx, cancel := f.StartNewReplay(ctx)
		result, err := replay.Execute(replayCtx, callableFlow, args)
		cancel(nil)
		if ctx.Err() != nil {
			// if the context is done, comply by returning immediately
			return result, ctx.Err()
		} else if errors.Is(context.Cause(replayCtx), fcontext.ErrRestartReplay) {
			// special case to restart the replay without performing the default end of replay behavior.
			goto restartReplay
		} else if err == nil {
			return result, nil
		}

		// if we encounter any other error, handle it
		if opts.Hooks.OnError != nil {
			for _, onError := range opts.Hooks.OnError {
				continueExecution := onError(err)
				// short circuit if any onError handler returns false
				if !continueExecution {
					return result, err
				}
			}
		}
	}
}
