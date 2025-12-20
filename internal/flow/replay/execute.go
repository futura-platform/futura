package replay

import (
	"context"
)

// this serves as a stable marker for the framework to
// find the closest user defined caller frame below the flow function, at runtime.
func Execute[A, T any](
	ctx context.Context,
	callableFlow func(ctx context.Context, args A) (T, error),
	args A,
) (result T, err error) {
	return callableFlow(ctx, args)
}
