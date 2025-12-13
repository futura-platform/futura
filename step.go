package futura

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

type StepOptions struct {
	// A human readable "label" of the step. This is used to identify the step in logs/traces.
	// Default: inferred from the function name using reflection.
	Label string
}

type StepFn[T comparable] func(ctx context.Context) (T, error)

// Step is a function that executes a step in the flow.
// It memoizes the result of the step and returns it if the step is called again at the same "moment" in the flow.
// The inner function must return a pointer to a comparable type, this is to enforce immutability of the result.
func Step[T comparable](ctx context.Context, opts *StepOptions, fn StepFn[T]) (T, error) {
	f := mustGetFlowContext(ctx)
	defer func() {
		if err := recover(); err != nil {
			panic(err)
		} else {
			// if no panic, increment the sequence index
			f.sequenceIndex++
		}
	}()

	if opts == nil {
		opts = &StepOptions{}
	}

	if opts.Label == "" {
		opts.Label = inferStepLabel(fn)
	}

	fnPc := reflect.ValueOf(fn).Pointer()

	if f.sequenceIndex > len(f.memoizedMomentSequence) {
		panic("inconsistent state: sequenceIndex is greater than the length of the memoized moment sequence")
	} else if f.sequenceIndex == len(f.memoizedMomentSequence) {
		// handle for the case where we have not executed this moment yet, so we need to execute the function and memoize the result
		nextMoment := moment{
			fnPointer: fnPc,
		}
		result, err := fn(ctx)
		if err != nil {
			return result, err
		}

		nextMoment.result = result
		f.memoizedMomentSequence = append(f.memoizedMomentSequence, nextMoment)
	}

	// handle for the case where we have already executed this moment, so we can return the memoized result
	existingMoment := f.memoizedMomentSequence[f.sequenceIndex]
	if existingMoment.fnPointer != fnPc {
		panic("inconsistent state: fnPointer of the existing moment does not match the fnPointer of the current function")
	}

	result, ok := existingMoment.result.(T)
	if !ok {
		panic(fmt.Sprintf("inconsistent state: result is of type '%T', expected '%T'", existingMoment.result, result))
	}
	return result, nil
}

func inferStepLabel[T comparable](fn StepFn[T]) string {
	pc := reflect.ValueOf(fn).Pointer()
	f := runtime.FuncForPC(pc)

	fullName := f.Name()
	parts := strings.Split(fullName, ".")
	return parts[len(parts)-1]
}
