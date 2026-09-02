package flow

import (
	"context"
	"errors"
	"fmt"

	"github.com/futura-platform/futura/ftype"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/flow/replay"
	"github.com/futura-platform/futura/internal/flow/replay/sequence"
	"github.com/futura-platform/futura/internal/flowhooks"
	"github.com/futura-platform/futura/internal/step"
)

type CallableFlow[A, T any] func(ctx context.Context, args A) (T, error)

var (
	ErrOccurredOutsideOfEvaluation = errors.New("error occurred outside of step evaluation")
	ErrPendingInvalidationAtSettle = errors.New("a replay settled with a pending sequence invalidation")
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
		var dirtyEpoch uint64
		replayCtx, dirtyEpoch = f.StartNewReplay(ctx)
		result, err := replay.Execute(replayCtx, callableFlow, args)
		replay.Cancel(replayCtx, nil)

		switch {
		case ctx.Err() != nil:
			// if the context is done, comply by returning immediately
			return result, ctx.Err()
		case errors.Is(context.Cause(replayCtx), execution.ErrRestartReplay):
			// special case to always restart the replay, even if otherwise the result, err combo would be terminal
			// if the replay was restarted, the sequence has NOT been settled, so we need to skip the settle step.
			f.CommitInvalidation(ctx)
			continue
		case errors.Is(err, ftype.ErrCancelFlow):
			// special case to immedieately return the error from the loop.
			return result, err
		case err != nil && !errors.Is(err, step.ErrEvalFailed):
			// if this error did not come from a step evaluation failure, the flow loop should be broken.
			// Since the flow fn is expected to be pure outside of steps, any error is expected to be unrecoverable.
			return result, fmt.Errorf("%w: %w", ErrOccurredOutsideOfEvaluation, err)
		}

		// now that all the terminal failure states have been handled, we can settle the sequence.
		if f.HasPendingInvalidation() {
			// an invalidation always restarts the replay, this should never happen.
			panic(ftrerrors.InconsistentStateError(ErrPendingInvalidationAtSettle))
		}
		// A failed step records its call order entry without advancing the index (to enforce strictness on the follow up call),
		// so the index is the last recorded entry on failure but one past it on success.
		lastRecordedIndex := sequence.GetIndex(replayCtx)
		if err == nil {
			lastRecordedIndex--
		}
		f.SettleSequence(ctx, lastRecordedIndex, dirtyEpoch)

		if err == nil {
			return result, nil
		}
	}
}
