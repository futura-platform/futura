package fopt

import (
	"context"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/flow/execution"
)

// WithCodeVersion declares the version of the code the flow runs under. A version the container has
// not run under before relaxes the strictness of the next replay, the same as a state change, so the
// flow may take branches the recorded call order did not.
func WithCodeVersion(version string) ftype.FlowLoopOption {
	return func(ctx context.Context) context.Context {
		return execution.WithCodeVersion(ctx, version)
	}
}
