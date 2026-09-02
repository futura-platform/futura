package moment

import (
	"context"
	"reflect"
	"runtime"

	"github.com/futura-platform/futura/ftype"
)

type Callable[A any, R any] func(ctx context.Context, args A) (R, error)

// Fn is an immutable structure representing an instance of a (potentially) impure function.
type Fn[A any, R any] struct {
	// the caller frame of the closest flow function
	// TODO: add a runtime assertion to check that no 2 Fn's have the same flowCaller
	// in the same flowContext.memoizedMomentSequence.
	// this might occur if a flow loop is used with a wrapper function that calls the actual flow function, like this:
	/*
		futura.NewFlow(ctx, func(ctx context.Context, args A) error {
			return actualFlowFunction(ctx, args)
		}, args)
	*/
	flowCaller runtime.Frame
	callable   Callable[A, R]

	options  []ftype.MomentFnOption
	metadata ftype.MomentFnMetadata
}

func NewFn[A comparable, R any](callable Callable[A, R], options ...ftype.MomentFnOption) Fn[A, R] {
	c := Fn[A, R]{callable: callable, options: options}
	for _, opt := range append([]ftype.MomentFnOption{ftype.WithLabel(CompileTimeLabel(c.runtimeFunc()))}, options...) {
		opt(&c.metadata)
	}
	return c
}

func (fn Fn[A, R]) Call(ctx context.Context, identity Identity, args A) (R, error) {
	return fn.callable(context.WithValue(ctx, currentIdentityKey{}, identity), args)
}

func (fn Fn[A, R]) Label() string {
	return fn.metadata.Label
}

func (fn Fn[A, R]) Options() []ftype.MomentFnOption {
	return fn.options
}

func (fn Fn[A, R]) runtimeFunc() *runtime.Func {
	return runtime.FuncForPC(reflect.ValueOf(fn.callable).Pointer())
}
