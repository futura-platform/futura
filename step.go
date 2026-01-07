package futura

import (
	"context"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/flow/moment"
	"github.com/futura-platform/futura/internal/step"
)

// ComparableMomentFn is a function that is has comparable inputs and returns a comparable type.
// This is to ensure deep memoization of the inputs is done correctly,
// and to enforce immutability of the output.
type ComparableMomentFn[A comparable, R comparable] func(ctx context.Context, args A) (R, error)

// Step is a function that executes a step in the flow.
// It memoizes the result of the step and returns it if the step is called again at the same "moment" in the flow.
func Step[A comparable, R comparable](b FlowBuilder, fn ComparableMomentFn[A, R], args A, options ...ftype.MomentFnOption) (output R, err error) {
	output, _, err = step.Evaluate(
		b,
		moment.NewFn(
			moment.Callable[A, R](fn),
			options...,
		),
		args,
	)
	return output, err
}
