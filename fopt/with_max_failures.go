package fopt

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype"
)

var ErrMaxFailuresReached = errors.New("max failures exceeded")

var failureCountHandle = futura.NewPlainDurableHandle("failureCount", func() *atomic.Int32 { return new(atomic.Int32) })

func WithMaxFailures(maxFailures int32) ftype.FlowLoopOption {
	return func(ctx context.Context) context.Context {
		countedCtx := failureCountHandle.ProvideContext(ctx)
		return WithStepWrapper(func(ctx context.Context, fnLabel string, args any, callstack []runtime.Frame, call func() (output any, err error)) (errOverride error) {
			_, err := call()
			if err != nil {
				newFailureCount := failureCountHandle.Use(countedCtx).Add(1)
				if newFailureCount > maxFailures {
					return fmt.Errorf("%w: %w: %d, last error: %w", ftype.ErrCancelFlow, ErrMaxFailuresReached, newFailureCount, err)
				}
			}
			return nil
		})(countedCtx)
	}
}
