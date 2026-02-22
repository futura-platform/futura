package privateencoding

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

type registryNamedType struct {
	Value int
}

func TestTypeRegistrationName(t *testing.T) {
	t.Run("named_type_uses_pkg_path", func(t *testing.T) {
		rt := reflect.TypeFor[registryNamedType]()
		expected := rt.PkgPath() + "." + rt.Name()
		assert.Equal(t, expected, typeRegistrationName(rt))
	})

	t.Run("pointer_named_type_keeps_string_form", func(t *testing.T) {
		rt := reflect.TypeFor[*registryNamedType]()
		assert.Equal(t, rt.String(), typeRegistrationName(rt))
	})

	t.Run("unnamed_type_uses_string_form", func(t *testing.T) {
		rt := reflect.TypeOf(struct{ Value int }{})
		assert.Equal(t, rt.String(), typeRegistrationName(rt))
	})
}

func TestRegister(t *testing.T) {
	t.Run("interface_types_panic", func(t *testing.T) {
		assert.PanicsWithValue(t, "interface types cannot be registered", func() {
			Register[any]()
		})
	})
}
