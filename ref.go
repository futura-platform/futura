package futura

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/flow/fcontext"
	"github.com/futura-platform/futura/internal/flow/moment"
	"github.com/futura-platform/futura/internal/step"
)

func refWithInitialValue[T comparable](b FlowBuilder, initialValue T, options ...ftype.MomentFnOption) (ref *T) {
	f := fcontext.MustFromContext(b)
	options = append([]ftype.MomentFnOption{ftype.WithLabel(fmt.Sprintf(
		"%T-ref[%d](%v)",
		initialValue,
		f.SequenceIndex(),
		initialValue,
	))}, options...)
	// use a step moment to create a pointer to the state,
	// using that pointer as the return value so that it gets used as the memoization key.
	ref, _, err := step.Evaluate(
		b,
		moment.NewFn(
			func(ctx context.Context, iv T) (*T, error) {
				return &iv, nil
			},
			options...,
		),
		initialValue,
	)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			// if the context is canceled, just return a zero value
			return new(T)
		}
		panic(err)
	}
	return ref
}

// Ref is a function that allows a stateful value to be defined and updated within a flow.
// This value's changes will persist across replays of the flow.
// A change in the initial value will cause the state to be re-initialized to that new value.
// If an initial value is provided, it will be used as the initial value of the state.
// If no initial value is provided, the state will be initialized to the zero value of the type.
// Passing more than one initial value will panic.
func Ref[T comparable](b FlowBuilder, initialValue ...T) (ref *T) {
	switch len(initialValue) {
	case 0:
		t := reflect.TypeOf((*T)(nil)).Elem()
		return refWithInitialValue(b, reflect.Zero(t).Interface().(T))
	case 1:
		return refWithInitialValue(b, initialValue[0])
	default:
		panic(fmt.Sprintf("State can only be called with 1 initial value argument, got %d", len(initialValue)))
	}
}
