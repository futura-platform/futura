package privateencoding

import (
	htmltemplate "html/template"
	"reflect"
	"testing"
	texttemplate "text/template"

	"github.com/futura-platform/futura/privateencoding/internal/otherpackage"
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

	t.Run("an unnamed struct qualifies its fields", func(t *testing.T) {
		// reflect spells a field's type with its short package name, so two packages named template
		// would collide
		text := reflect.TypeOf(struct{ T *texttemplate.Template }{})
		html := reflect.TypeOf(struct{ T *htmltemplate.Template }{})
		assert.Equal(t, text.String(), html.String())
		assert.NotEqual(t, typeRegistrationName(text), typeRegistrationName(html))
	})

	t.Run("an unnamed struct qualifies its unexported fields by their package", func(t *testing.T) {
		here, there := reflect.TypeOf(struct{ id int }{}), otherpackage.UnexportedFieldType()
		assert.NotEqual(t, typeRegistrationName(here), typeRegistrationName(there))
	})

	t.Run("an unnamed interface qualifies its unexported methods by their package", func(t *testing.T) {
		here, there := reflect.TypeFor[interface{ id() }](), otherpackage.UnexportedMethodType()
		assert.NotEqual(t, typeRegistrationName(here), typeRegistrationName(there))
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
