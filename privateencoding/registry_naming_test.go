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

	t.Run("pointer_to_named_type_is_qualified", func(t *testing.T) {
		rt := reflect.TypeFor[*registryNamedType]()
		expected := "*" + rt.Elem().PkgPath() + "." + rt.Elem().Name()
		assert.Equal(t, expected, typeRegistrationName(rt))
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

type localNameA struct{ A int }

func TestRegistry_UnnamedCompositeTypesAreQualified(t *testing.T) {
	// an unnamed composite of a named type is named through the named type's import path, so two
	// packages' models.User do not collide on "*models.User"
	name := typeRegistrationName(reflect.TypeFor[*localNameA]())
	assert.Contains(t, name, reflect.TypeFor[localNameA]().PkgPath())
	name = typeRegistrationName(reflect.TypeFor[map[string][]localNameA]())
	assert.Contains(t, name, reflect.TypeFor[localNameA]().PkgPath())
}
