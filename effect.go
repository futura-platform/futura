package futura

import (
	"context"
	"reflect"
	"runtime"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/moment"
)

// EffectFn is a function that is has comparable inputs and returns an error.
// It is the same as ComparableMoment, but without a return value.
type EffectFn[A comparable] func(ctx context.Context, args A) error

// Effect is a helper function for steps that don't return a value.
// It is the same as if you called Step and ignored the return value.
func Effect[A comparable](b FlowBuilder, fn EffectFn[A], args A, options ...ftype.MomentFnOption) error {
	fnPc := runtime.FuncForPC(reflect.ValueOf(fn).Pointer())
	opts := append(
		[]ftype.MomentFnOption{ftype.WithLabel(moment.CompileTimeLabel(fnPc))},
		options...,
	)
	_, err := Step(b, func(ctx context.Context, args A) (struct{}, error) {
		return struct{}{}, fn(ctx, args)
	}, args, opts...)
	return err
}

type ActionFn func(ctx context.Context) error

// Action is a helper function for Effects that don't have any arguments.
func Action(b FlowBuilder, fn ActionFn, options ...ftype.MomentFnOption) error {
	return Effect(b, func(ctx context.Context, _ struct{}) error {
		return fn(ctx)
	}, struct{}{}, options...)
}
