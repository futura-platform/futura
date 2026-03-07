package stepwrapper

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithStepWrapper(t *testing.T) {
	t.Run("attaches the wrapper directly to the context if there is no parent wrapper", func(t *testing.T) {
		wrapper := func(ctx context.Context, fnLabel string, args any, callstack []runtime.Frame, call func() (output any, err error)) (errOverride error) {
			return nil
		}
		ctx := With(t.Context(), wrapper)

		wrapperFromCtx, ok := FromContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, reflect.ValueOf(wrapper).Pointer(), reflect.ValueOf(wrapperFromCtx).Pointer())
	})

	t.Run("attaches the parented wrapper to the context if there is a parent wrapper. The parent is called before the child.", func(t *testing.T) {
		runParentChildTest := func(parentErr error, childErr error) error {
			callOrder := []string{}
			parentWrapper := func(ctx context.Context, fnLabel string, args any, callstack []runtime.Frame, call func() (any, error)) error {
				assert.Equal(t, "test-step", fnLabel)
				callOrder = append(callOrder, "parent")
				call()
				return parentErr
			}
			ctx := With(t.Context(), parentWrapper)
			childWrapper := func(ctx context.Context, fnLabel string, args any, callstack []runtime.Frame, call func() (any, error)) error {
				assert.Equal(t, "test-step", fnLabel)
				callOrder = append(callOrder, "child")
				call()
				return childErr
			}
			ctx = With(ctx, childWrapper)
			evalledStepWrapper, ok := FromContext(ctx)
			assert.True(t, ok)

			err := evalledStepWrapper(ctx, "test-step", nil, nil, func() (any, error) {
				callOrder = append(callOrder, "fn")
				return nil, nil
			})
			assert.Equal(t, []string{"parent", "child", "fn"}, callOrder)
			return err
		}
		t.Run("The parent error is returned if it is non nil and the child error is nil", func(t *testing.T) {
			expectedErr := errors.New("parent error")
			err := runParentChildTest(expectedErr, nil)
			assert.ErrorIs(t, err, expectedErr)
		})
		t.Run("The child error is returned if it is non nil and the parent error is nil", func(t *testing.T) {
			expectedErr := errors.New("child error")
			err := runParentChildTest(nil, expectedErr)
			assert.ErrorIs(t, err, expectedErr)
		})
		t.Run("The parent and child errors are chained if both are non nil", func(t *testing.T) {
			parentErr := errors.New("parent error")
			childErr := errors.New("child error")
			err := runParentChildTest(parentErr, childErr)
			assert.ErrorIs(t, err, parentErr)
			assert.ErrorIs(t, err, childErr)
		})
	})
}
