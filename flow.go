package futura

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/flow"
	"github.com/futura-platform/futura/internal/flow/fcontext"
)

type FlowFn[A, R any] func(b FlowBuilder, args A) (R, error)

var ErrTopLevelFlowConflict = errors.New("do not call futura.Flow from within a flow")

// Flow executes the flow fn, and is intended to be the entry point for a flow.
// It expects fn to be pure, except in child Step functions. It will continuously retry the flow until it is without error or the context is done.
func Flow[A, R any](ctx context.Context, fn FlowFn[A, R], args A, options ...ftype.FlowLoopOption) (result R, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch r := r.(type) {
			case error:
				err = fmt.Errorf("panic: %w", r)
			default:
				err = fmt.Errorf("panic: %v", r)
			}
			err = fmt.Errorf("%w\n%s", err, debug.Stack())
		}
	}()
	_, ok := fcontext.FromContext(ctx)
	if ok {
		return *new(R), ErrTopLevelFlowConflict
	}

	return flow.Loop(
		fcontext.WithFlow(ctx, options),
		func(flowCtx context.Context, args A) (R, error) {
			return fn(FlowBuilder{unexportedContext{flowCtx}}, args)
		},
		args,
	)
}
