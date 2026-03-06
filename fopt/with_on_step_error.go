package fopt

import (
	"context"
	"fmt"
	"runtime"

	"github.com/futura-platform/futura/ftype"
)

func WithOnStepError(onError func(ctx context.Context, err error) (continueExecution bool)) ftype.FlowLoopOption {
	return WithStepWrapper(func(ctx context.Context, args any, callstack []runtime.Frame, call func() (output any, err error)) (errOverride error) {
		_, err := call()
		if err != nil && !onError(ctx, err) {
			return fmt.Errorf("%w: %w", ftype.ErrCancelFlow, err)
		}
		return nil
	})
}
