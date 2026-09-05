package fopt

import (
	"context"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/flowhooks"
)

// WithOnExecutionEnd registers a callback that will be invoked once when a top-level
// flow execution ends (success or error).
//
// If multiple callbacks are registered, they are invoked in reverse order of
// registration (LIFO), mirroring Go's defer behavior.
//
// The callback's returned error (if any) will be joined onto the final flow error
// by the top-level flow runner.
func WithOnExecutionEnd(onEnd func(ctx context.Context, err error) error) ftype.FlowLoopOption {
	return flowhooks.WithOnExecutionEnd(onEnd)
}
