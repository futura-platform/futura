package futura

import (
	"context"
	"reflect"
	"runtime"

	"github.com/futura-platform/futura/ftype"
)

// EffectFn is a function that is has comparable inputs and returns an error.
// It is the same as ComparableMoment, but without a return value.
type EffectFn[A comparable] func(ctx context.Context, args A) error

// withRuntimeFuncOption binds the moment to the user's fn rather than to the wrapper the helper passes to Step.
func withRuntimeFuncOption(fn any, options ...ftype.MomentFnOption) []ftype.MomentFnOption {
	return append(
		[]ftype.MomentFnOption{ftype.WithRuntimeFunc(runtime.FuncForPC(reflect.ValueOf(fn).Pointer()))},
		options...,
	)
}

// Effect is a helper function for steps that don't return a value.
// It is the same as if you called Step and ignored the return value.
func Effect[A comparable](b FlowBuilder, fn EffectFn[A], args A, options ...ftype.MomentFnOption) error {
	opts := withRuntimeFuncOption(fn, options...)
	_, err := Step(b, func(ctx context.Context, args A) (struct{}, error) {
		return struct{}{}, fn(ctx, args)
	}, args, opts...)
	return err
}

// SourceFn is a function that has no inputs and returns a comparable output.
type SourceFn[R any] func(ctx context.Context) (R, error)

// Source is a helper function for steps that don't take any arguments.
func Source[R any](b FlowBuilder, fn SourceFn[R], options ...ftype.MomentFnOption) (output R, err error) {
	opts := withRuntimeFuncOption(fn, options...)
	return Step(b, func(ctx context.Context, _ struct{}) (R, error) {
		return fn(ctx)
	}, struct{}{}, opts...)
}

type ActionFn func(ctx context.Context) error

// Action is a helper function for steps that don't take any arguments and don't return a value.
func Action(b FlowBuilder, fn ActionFn, options ...ftype.MomentFnOption) error {
	opts := withRuntimeFuncOption(fn, options...)
	_, err := Step(b, func(ctx context.Context, _ struct{}) (struct{}, error) {
		return struct{}{}, fn(ctx)
	}, struct{}{}, opts...)
	return err
}
