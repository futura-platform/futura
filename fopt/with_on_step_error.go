package fopt

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/step"
)

func WithOnStepError(onError func(ctx context.Context, fnLabel string, callstack []runtime.Frame, err error) (continueExecution bool)) ftype.FlowLoopOption {
	return WithStepWrapper(func(ctx context.Context, fnLabel string, args any, callstack []runtime.Frame, call func() (output any, err error)) (errOverride error) {
		_, err := call()
		if err != nil && !errors.Is(err, step.ErrReplayTerminated) && !onError(ctx, fnLabel, callstack, err) {
			return fmt.Errorf("%w: %w", ftype.ErrCancelFlow, err)
		}
		return nil
	})
}
