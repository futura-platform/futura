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
	// what the call panicked with, if it did not return: a wrapper may have recovered it
	var callPanic any
	callFn := func() (any, error) {
		callCount++
		defer func() {
			if callPanic = recover(); callPanic != nil {
				panic(callPanic)
			}
		}()
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
		} else if callPanic != nil {
			// the wrapper recovered the call's panic. A termination is the runtime's own and is re-raised;
			// anything else was the step's, and a wrapper may not turn it into a return.
			if _, terminated := AsReplayTerminated(callPanic); terminated {
				panic(callPanic)
			}
			panic(fmt.Errorf("%w: %w: %v", ErrIllegalStepWrapperBehavior, ErrRecoveredCall, callPanic))
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

// AsReplayTerminated returns the replay termination in a recovered panic value, possibly wrapped by a
// step wrapper that re-panicked it inside its own error.
func AsReplayTerminated(recovered any) (terminated ReplayTerminatedError, ok bool) {
	err, isErr := recovered.(error)
	if !isErr {
		return terminated, false
	}
	return terminated, errors.As(err, &terminated)
}

func terminateIfReplayCancelled(ctx context.Context) {
	if replay.Err(ctx) != nil {
		panic(ReplayTerminatedError{Replay: ctx})
	}
}
