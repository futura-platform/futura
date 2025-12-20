package moment

import (
	"context"
	"reflect"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func labelToInfer(_ context.Context, _ *any) (any, error) {
	return nil, nil
}

func labelToInfer2[T any](_ context.Context, _ *T) (*T, error) {
	return nil, nil
}

func labelToInfer3[T1 any, T2 any](_ context.Context, _ *T1) (*T1, error) {
	return nil, nil
}

func runtimeFunc(fn any) *runtime.Func {
	return runtime.FuncForPC(reflect.ValueOf(fn).Pointer())
}

func TestFnCompileTimeLabel(t *testing.T) {
	t.Run("compile time label with 0 type params", func(t *testing.T) {
		label := CompileTimeLabel(runtimeFunc(labelToInfer))
		if label != "labelToInfer" {
			t.Errorf("expected labelToInfer, got %s", label)
		}

		label = CompileTimeLabel(runtimeFunc(func(ctx context.Context, args *any) (any, error) { return nil, nil }))
		assert.Equal(t, "1", label)
	})

	t.Run("compile time label with 1 type param", func(t *testing.T) {
		label := CompileTimeLabel(runtimeFunc(labelToInfer2[int]))
		assert.Equal(t, "labelToInfer2", label)
	})

	t.Run("compile time label with 2 type params", func(t *testing.T) {
		label := CompileTimeLabel(runtimeFunc(labelToInfer3[int, string]))
		assert.Equal(t, "labelToInfer3", label)
	})
}
