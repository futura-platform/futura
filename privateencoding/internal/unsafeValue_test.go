package privateencodinginternal_test

import (
	"reflect"
	"testing"

	privateencodinginternal "github.com/futura-platform/futura/privateencoding/internal"
	"github.com/futura-platform/futura/privateencoding/internal/otherpackage"
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
	t.Run("panics if the value can't interface and is not addressable", func(t *testing.T) {
		v := reflect.ValueOf(struct{ hidden int }{hidden: 42}).FieldByName("hidden")
		assert.False(t, v.CanInterface())
		assert.False(t, v.CanAddr())
		assert.Panics(t, func() { privateencodinginternal.UnsafeValue(v) })
	})
}

func assertNoKindChange[T any](t *testing.T) {
	v := reflect.ValueOf(new(T)).Elem()
	assert.NotEqual(t, v.Kind(), reflect.Pointer)
	assert.Equal(t, v.Kind(), privateencodinginternal.UnsafeValue(v).Kind())
}
