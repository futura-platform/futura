package privateencodinginternal_test

import (
	"reflect"
	"testing"

	privateencodinginternal "github.com/futura-platform/futura/internal/privateencoding/internal"
	"github.com/futura-platform/futura/internal/privateencoding/internal/otherpackage"
	"github.com/stretchr/testify/assert"
)

func TestUnsafeValue(t *testing.T) {
	t.Run("can set and get unexported field", func(t *testing.T) {
		v := reflect.ValueOf(otherpackage.NewMyStruct(42, 42))
		vf := v.Elem().FieldByName("unexportedField")
		assert.PanicsWithValue(t, "reflect.Value.Interface: cannot return value obtained from unexported field or method", func() {
			vf.Interface()
		})
		unsafeVf := privateencodinginternal.UnsafeValue(vf)
		assert.Equal(t, 42, unsafeVf.Interface())

		unsafeVf.SetInt(43)
		assert.Equal(t, 43, unsafeVf.Interface())
	})
	t.Run("doesn't change kind", func(t *testing.T) {
		assertNoKindChange[any](t)
		assertNoKindChange[int](t)
		assertNoKindChange[string](t)
		assertNoKindChange[[]any](t)
		assertNoKindChange[map[any]any](t)
		assertNoKindChange[struct{}](t)
		assertNoKindChange[chan any](t)
		assertNoKindChange[func()](t)
	})
}

func assertNoKindChange[T any](t *testing.T) {
	v := reflect.ValueOf(new(T)).Elem()
	assert.NotEqual(t, v.Kind(), reflect.Pointer)
	assert.Equal(t, v.Kind(), privateencodinginternal.UnsafeValue(v).Kind())
}
