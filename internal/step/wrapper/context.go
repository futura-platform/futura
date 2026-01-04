package stepwrapper

import (
	"context"
	"fmt"
	"runtime"
)

type contextKey string

const (
	stepWrapperContextKey contextKey = "stepWrapper"
)

type StepWrapper func(
	ctx context.Context,
	args any,
	callstack []runtime.Frame,
	call func() (output any, err error),
) (errOverride error)

func With(ctx context.Context, stepWrapper StepWrapper) context.Context {
	withParentWrapper := stepWrapper
	if parentWrapper, ok := FromContext(ctx); ok {
		// wrap the wrapper that has just been passed in with the parent (if it exists)
		withParentWrapper = func(ctx context.Context, args any, callstack []runtime.Frame, call func() (any, error)) error {
			var childErr error
			parentErr := parentWrapper(ctx, args, callstack, func() (pendingOutput any, pendingErr error) {
				childErr = stepWrapper(ctx, args, callstack, func() (any, error) {
					pendingOutput, pendingErr = call()
					return pendingOutput, pendingErr
				})
				if childErr != nil {
					pendingErr = childErr
				}
				return
			})
			if childErr != nil && parentErr != nil {
				return fmt.Errorf("%w: %w", parentErr, childErr)
			} else if childErr != nil {
				return childErr
			}
			return parentErr
		}
	}
	return context.WithValue(ctx, stepWrapperContextKey, withParentWrapper)
}

func FromContext(ctx context.Context) (StepWrapper, bool) {
	if wrapper, ok := ctx.Value(stepWrapperContextKey).(StepWrapper); ok {
		return wrapper, true
	}
	return nil, false
}
