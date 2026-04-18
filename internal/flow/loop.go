package flow

import (
	"context"
	"errors"
	"fmt"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/flow/replay"
	"github.com/futura-platform/futura/internal/flow/replay/sequence"
	"github.com/futura-platform/futura/internal/flowhooks"
	"github.com/futura-platform/futura/internal/step"
)

type CallableFlow[A, T any] func(ctx context.Context, args A) (T, error)

var (
	ErrOccurredOutsideOfEvaluation = errors.New("error occurred outside of step evaluation")
)

// Loop implements the core logic of the flow. It is responsible for:
// - executing the flow fn
// - handling errors
// - rewinding the sequence
func Loop[A, T any](ctx context.Context, callableFlow CallableFlow[A, T], args A, opts ...ftype.FlowLoopOption) (_ T, err error) {
	f := execution.MustFromContext(ctx)
	for _, opt := range opts {
		ctx = opt(ctx)
	}

	defer func() {
		// Run execution-end callbacks.
		if hookErr := flowhooks.RunOnExecutionEnd(ctx, err); hookErr != nil {
			err = errors.Join(err, fmt.Errorf("execution end hooks: %w", hookErr))
		}
	}()

	var replayCtx context.Context
	defer func() {
		if replayCtx != nil {
			sequence.RunDeferred(replayCtx)
		}
	}()

	for {
		replayCtx = f.StartNewReplay(ctx)
		result, err := replay.Execute(replayCtx, callableFlow, args)
		replay.Cancel(replayCtx, nil)

		if ctx.Err() != nil {
			// if the context is done, comply by returning immediately
			return result, ctx.Err()
		} else if errors.Is(context.Cause(replayCtx), execution.ErrRestartReplay) {
			// special case to always restart the replay, even if otherwise the result,err combo would be terminal
		} else if err == nil {
			return result, nil
		} else if errors.Is(err, ftype.ErrCancelFlow) {
			// special case to immedieately return the error from the loop.
			return result, err
		} else if !errors.Is(err, step.ErrEvalFailed) {
			// if this error did not come from a step evaluation failure, the flow loop should be broken.
			// Since the flow fn is expected to be pure outside of steps, any error is expected to be unrecoverable.
			return result, fmt.Errorf("%w: %w", ErrOccurredOutsideOfEvaluation, err)
		}

		// perform eviction here, after potential replays are completed
		f.EvictUnseenCachedMoments(replayCtx)
	}
}
