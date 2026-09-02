package step

import (
	"context"
	"errors"
	"runtime"
	"testing"

	stepwrapper "github.com/futura-platform/futura/internal/step/wrapper"
	"github.com/futura-platform/futura/internal/utils/testutil"
	"github.com/futura-platform/futura/moment"
	"github.com/stretchr/testify/assert"
)

func TestCall(t *testing.T) {
	t.Run("calls the fn without a wrapper, returning the result and error", func(t *testing.T) {
		callCount := 0
		expectedError := errors.New("expected error")
		fn := moment.NewFn(func(ctx context.Context, args struct{}) (string, error) {
			callCount++
			return "result", expectedError
		})
		output, err := call(t.Context(), fn, moment.Identity{}, struct{}{}, nil)
		assert.Equal(t, "result", output)
		assert.ErrorIs(t, err, expectedError)
		assert.Equal(t, 1, callCount)
	})

	t.Run("calls the fn with an illegal wrapper that does not call the fn", func(t *testing.T) {
		expectedError := errors.New("expected error")
		fn := moment.NewFn(func(ctx context.Context, args struct{}) (string, error) {
			return "result", expectedError
		})
		ctx := stepwrapper.With(t.Context(), func(ctx context.Context, fnLabel string, args any, callstack []runtime.Frame, call func() (output any, err error)) (errOverride error) {
			return nil
		})
		testutil.PanicsWithErrorIs(t, ErrDidNotCall, func() {
			call(ctx, fn, moment.Identity{}, struct{}{}, nil)
		})
	})

	t.Run("calls the fn with an illegal wrapper that calls the fn multiple times", func(t *testing.T) {
		callCount := 0
		expectedError := errors.New("expected error")
		fn := moment.NewFn(func(ctx context.Context, args struct{}) (string, error) {
			callCount++
			return "result", expectedError
		})
		ctx := stepwrapper.With(t.Context(), func(ctx context.Context, fnLabel string, args any, callstack []runtime.Frame, call func() (output any, err error)) (errOverride error) {
			call()
			call()
			return nil
		})
		testutil.PanicsWithErrorIs(t, ErrCalledMultipleTimes, func() {
			call(ctx, fn, moment.Identity{}, struct{}{}, nil)
		})
	})

	t.Run("calls the fn with a transparent wrapper, returning the result and error", func(t *testing.T) {
		callCount := 0
		expectedError := errors.New("expected error")
		fn := moment.NewFn(func(ctx context.Context, args struct{}) (string, error) {
			callCount++
			return "result", expectedError
		})
		wrapperCallCount := 0
		var wrapperReceivedLabel string
		var wrapperReceivedOutput any
		var wrapperReceivedError error
		ctx := stepwrapper.With(t.Context(), func(ctx context.Context, fnLabel string, args any, callstack []runtime.Frame, call func() (output any, err error)) (errOverride error) {
			wrapperCallCount++
			wrapperReceivedLabel = fnLabel
			wrapperReceivedOutput, wrapperReceivedError = call()
			return nil
		})
		output, err := call(ctx, fn, moment.Identity{}, struct{}{}, nil)
		assert.Equal(t, 1, callCount)
		assert.Equal(t, 1, wrapperCallCount)
		assert.Equal(t, fn.Label(), wrapperReceivedLabel)
		assert.Equal(t, "result", output)
		assert.Equal(t, "result", wrapperReceivedOutput)
		assert.ErrorIs(t, err, expectedError)
		assert.ErrorIs(t, wrapperReceivedError, expectedError)
	})

	t.Run("calls the fn with a wrapper that returns an error, causing the error returned by the fn to be overridden", func(t *testing.T) {
		expectedError := errors.New("expected error")
		fn := moment.NewFn(func(ctx context.Context, args struct{}) (string, error) {
			return "result", errors.New("unexpected error")
		})
		ctx := stepwrapper.With(t.Context(), func(ctx context.Context, fnLabel string, args any, callstack []runtime.Frame, call func() (output any, err error)) (errOverride error) {
			call()
			return expectedError
		})
		output, err := call(ctx, fn, moment.Identity{}, struct{}{}, nil)
		assert.Equal(t, "result", output)
		assert.ErrorIs(t, err, expectedError)
	})

	t.Run("calls an fn that adds a goroutine that is not removed before the fn returns, causing the step to fail", func(t *testing.T) {
		fn := moment.NewFn(func(ctx context.Context, args struct{}) (string, error) {
			ActiveGoroutinesFrom(ctx).Add(1)
			return "result", nil
		})
		testutil.PanicsWithErrorIs(t, ErrGoroutinesNotExited, func() {
			call(t.Context(), fn, moment.Identity{}, struct{}{}, nil)
		})
	})
}
