package futura

import (
	"context"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/step"
	"github.com/futura-platform/futura/moment"
)

// ComparableMomentFn is a function that is has comparable inputs
// (to ensure deep memoization of the inputs is done correctly)
// and returns an immutable value.
type ComparableMomentFn[A comparable, R any] func(ctx context.Context, args A) (R, error)

// Step is a function that executes a step in the flow.
// It memoizes the result of the step and returns it if the step is called again at the same "moment" in the flow.
func Step[A comparable, R any](b FlowBuilder, fn ComparableMomentFn[A, R], args A, options ...ftype.MomentFnOption) (output R, err error) {
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
