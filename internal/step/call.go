package step

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"github.com/futura-platform/futura/internal/flow/replay"
	stepwrapper "github.com/futura-platform/futura/internal/step/wrapper"
	"github.com/futura-platform/futura/moment"
)

var (
	ErrIllegalStepWrapperBehavior = errors.New("illegal step wrapper behavior")
	ErrDidNotCall                 = errors.New("did not call")
	ErrCalledMultipleTimes        = errors.New("called multiple times")
	ErrRecoveredCall              = errors.New("recovered a panic from the call")
	ErrGoroutinesNotExited        = errors.New("goroutines not exited")
)

func call[A comparable, R any](ctx context.Context, fn moment.Fn[A, R], identity moment.Identity, args A, callstack []runtime.Frame) (output R, err error) {
	callCount := 0
	callPanicked := true
	callFn := func() (any, error) {
		callCount++
		activeGoroutines, ctx := withActiveGoroutines(ctx)
		// make sure to assign to these variables (output, err) in the correct scope (NOT THE LOCAL SCOPE OF THIS FUNCTION)
		output, err = fn.Call(ctx, identity, args)
		if activeGoroutines.Cardinality() != 0 {
			panic(fmt.Errorf("%w: %s", ErrGoroutinesNotExited, activeGoroutines))
		}
		if errors.Is(err, ctx.Err()) {
			// the step returned the cancellation itself, rather than a verdict of its own.
			// if the cancellation was the replay's, terminate: the cancellation is not a failure.
			terminateIfReplayCancelled(ctx)
		}
		callPanicked = false
		return output, err
	}
	stepWrapper, ok := stepwrapper.FromContext(ctx)
	if !ok {
		callFn()
	} else {
		errOverride := stepWrapper(ctx, fn.Label(), args, callstack, callFn)
		if callCount == 0 {
			panic(fmt.Errorf("%w: %w", ErrIllegalStepWrapperBehavior, ErrDidNotCall))
		} else if callCount > 1 {
			panic(fmt.Errorf("%w: %w", ErrIllegalStepWrapperBehavior, ErrCalledMultipleTimes))
		} else if callPanicked {
			terminateIfReplayCancelled(ctx)
			panic(fmt.Errorf("%w: %w", ErrIllegalStepWrapperBehavior, ErrRecoveredCall))
		} else if errOverride != nil {
			err = errOverride
		}
	}
	if err != nil {
		return
	}

	return output, nil
}

// ErrReplayTerminated is the panic value that "terminates" a cancelled replay.
// "terminate"-ing is a mechanism to immediately stop a replay without having to
// propagate the error through user code.
var ErrReplayTerminated = errors.New("replay terminated")

// ReplayTerminatedError is the panic that terminates a replay. It records which replay was
// observed as cancelled, since a step may have been reached through a builder from another replay.
type ReplayTerminatedError struct {
	// Replay is the context of the replay that was observed as cancelled.
	Replay context.Context
}

func (e ReplayTerminatedError) Error() string {
	return fmt.Sprintf("%s: %s", ErrReplayTerminated, replay.Cause(e.Replay))
}

func (e ReplayTerminatedError) Is(target error) bool { return target == ErrReplayTerminated }

func (e ReplayTerminatedError) Unwrap() error { return replay.Cause(e.Replay) }

func terminateIfReplayCancelled(ctx context.Context) {
	if replay.Err(ctx) != nil {
		panic(ReplayTerminatedError{Replay: ctx})
	}
}
