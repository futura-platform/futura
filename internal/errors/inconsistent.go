package ftrerrors

import (
	"errors"
	"fmt"
)

var ErrInconsistentState = errors.New("inconsistent state")

func InconsistentStateError(subErr error) error {
	return fmt.Errorf("%w: %w", ErrInconsistentState, subErr)
}

// ErrFlowPanic reports a panic anywhere in an execution: the flow fn, a step, a wrapper, a
// deferred function, a handle's cleanup, or an execution end hook.
var ErrFlowPanic = errors.New("flow panicked")

// Recovering calls fn, and reports a panic in it as an error instead of unwinding.
// It is for running a sequence of user callbacks where one failing must not skip the rest.
func Recovering(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = PanicError(r)
		}
	}()
	return fn()
}

// PanicError converts a recovered panic value into an error that reports the panic.
func PanicError(recovered any) error {
	switch r := recovered.(type) {
	case error:
		return fmt.Errorf("%w: %w", ErrFlowPanic, r)
	default:
		return fmt.Errorf("%w: %v", ErrFlowPanic, r)
	}
}
