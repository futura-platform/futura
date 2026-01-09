package futura

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"unsafe"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/flow"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/privateencoding"
)

type FlowFn[A, R any] func(b FlowBuilder, args A) (R, error)

var (
	ErrTopLevelFlowConflict = errors.New("do not call futura.Flow from within a flow")
	ErrAlreadyRunning       = errors.New("flow is already running")
	ErrFlowPanic            = errors.New("flow panicked")
)

type Flow[A, R any] struct {
	running atomic.Bool
	exec    *execution.FlowExecution
}

// an internal helper to make sure later code doesn't forget to initialize new fields.
func _newFlow[A, R any](
	exec *execution.FlowExecution,
) *Flow[A, R] {
	return &Flow[A, R]{
		exec: exec,
	}
}

// SerializedFlow is a semantic type alias that clarifies that these bytes represent a flow.
type SerializedFlow []byte

// NewFlow creates a new flow, and is intended to be the entry point for a flow.
// It expects fn to be pure, except in child Step functions.
func NewFlow[A, R any]() *Flow[A, R] {
	return _newFlow[A, R](execution.NewFlowExecution())
}

func NewFlowFromSerialized[A, R any](serialized SerializedFlow) (*Flow[A, R], error) {
	dec := privateencoding.NewDecoder[execution.FlowExecutionState](bytes.NewReader(serialized))
	state, err := dec.Decode()
	if err != nil {
		return nil, err
	}

	return _newFlow[A, R](execution.NewFlowExecutionFromState(state)), nil
}

// Execute runs the flow execution loop, and is intended to be the entry point for a flow.
// It will continuously retry the flow until it is without error or the context is done.
// Any panics within the flow will be caught and returned as an error.
func (f *Flow[A, R]) Execute(ctx context.Context, fn FlowFn[A, R], args A, opts ...ftype.FlowLoopOption) (result R, err error) {
	if !f.running.CompareAndSwap(false, true) {
		return *new(R), ErrAlreadyRunning
	}
	defer f.running.Store(false)

	_, ok := execution.FromContext(ctx)
	if ok {
		return *new(R), ErrTopLevelFlowConflict
	}

	defer func() {
		if r := recover(); r != nil {
			switch r := r.(type) {
			case error:
				err = fmt.Errorf("%w: %w", ErrFlowPanic, r)
			default:
				err = fmt.Errorf("%w: %v", ErrFlowPanic, r)
			}
			err = fmt.Errorf("%w\n%s", err, debug.Stack())
		}
	}()

	result, err = flow.Loop(
		execution.WithFlow(ctx, f.exec),
		func(flowCtx context.Context, args A) (R, error) {
			return fn(FlowBuilder{unexportedContext{flowCtx}}, args)
		},
		args,
		opts...,
	)

	return result, err
}

func (f *Flow[A, R]) Serialize() (SerializedFlow, error) {
	state := f.exec.State()

	sizeHeuristic := unsafe.Sizeof(state)
	buf := bytes.NewBuffer(make([]byte, 0, sizeHeuristic))

	enc := privateencoding.NewEncoder[execution.FlowExecutionState](buf)
	if err := enc.Encode(state); err != nil {
		panic(err)
	}

	return buf.Bytes(), nil
}
