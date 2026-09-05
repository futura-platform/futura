package step

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"

	"github.com/futura-platform/futura/internal/flow/replay"
	stepwrapper "github.com/futura-platform/futura/internal/step/wrapper"
	"github.com/futura-platform/futura/moment"
	"github.com/petermattis/goid"
)

var (
	ErrIllegalStepWrapperBehavior = errors.New("illegal step wrapper behavior")
	ErrDidNotCall                 = errors.New("did not call")
	ErrCalledMultipleTimes        = errors.New("called multiple times")
	ErrDidNotReturn               = errors.New("returned before the call did")
	ErrRecoveredCall              = errors.New("recovered a panic from the call")
	// ErrStillRunning is raised when a step's code may still be running after the step ended: a leaked
	// goroutine, or a call the wrapper abandoned. Nothing of the step is recorded, since it may still be writing.
	ErrStillRunning        = errors.New("the step may still be running")
	ErrGoroutinesNotExited = fmt.Errorf("%w: goroutines not exited", ErrStillRunning)
)

// outcome is how a call of the step's fn ended.
type outcome[R any] struct {
	output R
	err    error
	// panicked is what the fn panicked with, if it did not return.
	panicked any
}

func call[A comparable, R any](ctx context.Context, fn moment.Fn[A, R], identity moment.Identity, args A, callstack []runtime.Frame) (output R, err error) {
	// the wrapper may run the call on another goroutine, so the outcome is published, not assigned to the frame
	var calls atomic.Int32
	var ended atomic.Pointer[outcome[R]]
	flowGoroutine := goid.Get()
	callFn := func() (any, error) {
		calls.Add(1)
		var o outcome[R]
		activeGoroutines, ctx := withActiveGoroutines(ctx)
		defer func() {
			o.panicked = recover()
			if activeGoroutines.Cardinality() != 0 {
				o.panicked = fmt.Errorf("%w: %s", ErrGoroutinesNotExited, activeGoroutines)
			}
			ended.Store(&o)
			// a panic is only re-raised on the flow's goroutine, where the runtime recovers it. On any other
			// it would kill the process, so it is reported once the wrapper returns
			if o.panicked != nil && goid.Get() == flowGoroutine {
				panic(o.panicked)
			}
		}()
		o.output, o.err = fn.Call(ctx, identity, args)
		if errors.Is(o.err, ctx.Err()) {
			// the step returned the cancellation itself, rather than a verdict of its own.
			// if the cancellation was the replay's, terminate: the cancellation is not a failure.
			terminateIfReplayCancelled(ctx)
		}
		return o.output, o.err
	}

	stepWrapper, ok := stepwrapper.FromContext(ctx)
	var errOverride error
	if !ok {
		callFn()
	} else {
		errOverride = stepWrapper(ctx, fn.Label(), args, callstack, callFn)
	}

	switch o := ended.Load(); {
	case calls.Load() == 0:
		panic(fmt.Errorf("%w: %w", ErrIllegalStepWrapperBehavior, ErrDidNotCall))
	case calls.Load() > 1:
		panic(fmt.Errorf("%w: %w", ErrIllegalStepWrapperBehavior, ErrCalledMultipleTimes))
	case o == nil:
		// if the wrapper called the fn in a goroutine and exitted before it, it may still be running.
		panic(fmt.Errorf("%w: %w: %w", ErrStillRunning, ErrIllegalStepWrapperBehavior, ErrDidNotReturn))
	case o.panicked != nil:
		// a step panic is either a runtime signal, or the step's own panic (which is terminal)
		if _, terminated := AsReplayTerminated(o.panicked); terminated {
			panic(o.panicked)
		}
		if perr, isErr := o.panicked.(error); isErr && errors.Is(perr, ErrStillRunning) {
			panic(o.panicked)
		}

		// attempt to recover the error type to preserve go error composition
		if perr, isErr := o.panicked.(error); isErr {
			panic(fmt.Errorf("%w: %w: %w", ErrIllegalStepWrapperBehavior, ErrRecoveredCall, perr))
		}
		panic(fmt.Errorf("%w: %w: %v", ErrIllegalStepWrapperBehavior, ErrRecoveredCall, o.panicked))
	case errOverride != nil:
		return o.output, errOverride
	default:
		return o.output, o.err
	}
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
