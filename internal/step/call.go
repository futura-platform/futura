package step

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	stepwrapper "github.com/futura-platform/futura/internal/step/wrapper"
	"github.com/futura-platform/futura/moment"
)

var (
	ErrIllegalStepWrapperBehavior = errors.New("illegal step wrapper behavior")
	ErrDidNotCall                 = errors.New("did not call")
	ErrCalledMultipleTimes        = errors.New("called multiple times")
	ErrGoroutinesNotExited        = errors.New("goroutines not exited")
)

func call[A comparable, R any](ctx context.Context, fn moment.Fn[A, R], args A, callstack []runtime.Frame) (output R, err error) {
	callCount := 0
	callFn := func() (any, error) {
		callCount++
		activeGoroutines, ctx := withActiveGoroutines(ctx)
		// make sure to assign to these variables (output, err) in the correct scope (NOT THE LOCAL SCOPE OF THIS FUNCTION)
		output, err = fn.Call(ctx, args)
		if activeGoroutines.Cardinality() != 0 {
			panic(fmt.Errorf("%w: %s", ErrGoroutinesNotExited, activeGoroutines))
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
		} else if errOverride != nil {
			err = errOverride
		}
	}
	if err != nil {
		return
	}

	return output, nil
}
