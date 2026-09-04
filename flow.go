package futura

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/futura-platform/futura/internal/durable"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/flowhooks"
)

type FlowFn[A, R any] func(b FlowBuilder, args A) (R, error)

var (
	ErrTopLevelFlowConflict = errors.New("do not call futura.Flow from within a flow")
	ErrAlreadyRunning       = errors.New("flow is already running")
	// ErrFlowPanic reports a panic anywhere in an execution: the flow fn, a step, a wrapper, a
	// deferred function, a handle's cleanup, or an execution end hook.
	ErrFlowPanic = ftrerrors.ErrFlowPanic
)

type Flow[A, R any] struct {
	exec *execution.FlowExecution
}

// an internal helper to make sure later code doesn't forget to initialize new fields.
func _newFlow[A, R any](
	exec *execution.FlowExecution,
) *Flow[A, R] {
	return &Flow[A, R]{
		exec: exec,
	}
}

// cleanupHandlesAtEnd releases the run's handles once the execution ends, whatever else was registered.
var cleanupHandlesAtEnd = flowhooks.WithOnExecutionEnd(func(ctx context.Context, _ error) error {
	return execution.MustFromContext(ctx).Handles().Cleanup()
})

// SerializedFlow is a semantic type alias that clarifies that these bytes represent a flow.
type SerializedFlow []byte

// NewFlow creates a new flow, and is intended to be the entry point for a flow.
// It expects fn to be pure, except in child Step functions.
func NewFlow[A, R any]() *Flow[A, R] {
	return _newFlow[A, R](execution.NewFlowExecution())
}

func NewFlowFromContainer[A, R any](c executiontype.TransactionalContainer) *Flow[A, R] {
	return _newFlow[A, R](execution.NewFlowExecutionWithContainer(c))
}

// Execute runs the flow execution loop, and is intended to be the entry point for a flow.
// It will continuously retry the flow until it is without error or the context is done.
// Any panics within the flow will be caught and returned as an error.
func (f *Flow[A, R]) Execute(ctx context.Context, fn FlowFn[A, R], args A, opts ...ftype.FlowLoopOption) (result R, err error) {
	stopRun, ok := f.exec.TryStartRun()
	if !ok {
		return *new(R), ErrAlreadyRunning
	}
	defer stopRun()

	// the loop reports the flow's panics as errors; this is the backstop for anything that escapes it
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w\n%s", ftrerrors.PanicError(r), debug.Stack())
		}
	}()

	if _, ok := execution.FromContext(ctx); ok {
		return *new(R), ErrTopLevelFlowConflict
	}

	result, err = flow.Loop(
		durable.WithHandles(execution.WithFlow(ctx, f.exec), f.exec.Handles()),
		func(flowCtx context.Context, args A) (R, error) {
			return fn(newFlowBuilder(flowCtx, f.exec), args)
		},
		args,
		append([]ftype.FlowLoopOption{cleanupHandlesAtEnd}, opts...)...,
	)

	return result, err
}
