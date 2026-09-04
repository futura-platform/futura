package privateencoding

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"github.com/puzpuzpuz/xsync/v4"
)

var (
	registeredTypeNameByType = xsync.NewMap[reflect.Type, string]()
	registeredTypeByName     = xsync.NewMap[string, reflect.Type]()

	registerMu sync.Mutex
)

// Register records a type name mapping used for interface serialization.
// Types serialized behind interface fields must be registered before decode.
func Register[T any]() {
	RegisterType(reflect.TypeFor[T]())
}

// RegisterType records rt under its registration name: the tag written for a value of rt behind an
// interface, and read back to pick the type to decode into. The tag must name exactly one type, so
// two distinct types with the same name are a hard error.
func RegisterType(rt reflect.Type) {
	if rt.Kind() == reflect.Interface {
		panic("interface types cannot be registered")
	}
	name := typeRegistrationName(rt)

	registerMu.Lock()
	defer registerMu.Unlock()
	if _, ok := registeredTypeNameByType.Load(rt); ok {
		return
	}
	if existing, ok := registeredTypeByName.Load(name); ok {
		panic(fmt.Sprintf(
			"privateencoding: two distinct types share the registration name %q (%s and %s); "+
				"a type declared inside a function is known only by its package and name, so give them distinct names",
			name, existing.String(), rt.String(),
		))
	}
	registeredTypeNameByType.Store(rt, name)
	registeredTypeByName.Store(name, rt)
}

func lookupRegisteredTypeName(rt reflect.Type) (string, bool) {
	return registeredTypeNameByType.Load(rt)
}

func lookupRegisteredType(typeName string) (reflect.Type, bool) {
	return registeredTypeByName.Load(typeName)
}

func init() {
	// Builtins are common behind interface values (e.g. any(1), map[any]... keys).
	Register[bool]()
	Register[int]()
	Register[int8]()
	Register[int16]()
	Register[int32]()
	Register[int64]()
	Register[uint]()
	Register[uint8]()
	Register[uint16]()
	Register[uint32]()
	Register[uint64]()
	Register[uintptr]()
	Register[float32]()
	Register[float64]()
	Register[complex64]()
	Register[complex128]()
	Register[string]()
	Register[[]byte]()
}

// typeRegistrationName creates a (best effort) unique name for the given type.
func typeRegistrationName(rt reflect.Type) string {
	if rt.Name() != "" {
		if rt.PkgPath() == "" {
			return rt.Name()
		}
		return rt.PkgPath() + "." + rt.Name()
	}
	switch rt.Kind() {
	case reflect.Pointer:
		return "*" + typeRegistrationName(rt.Elem())
	case reflect.Slice:
		return "[]" + typeRegistrationName(rt.Elem())
	case reflect.Array:
		return "[" + strconv.Itoa(rt.Len()) + "]" + typeRegistrationName(rt.Elem())
	case reflect.Map:
		return "map[" + typeRegistrationName(rt.Key()) + "]" + typeRegistrationName(rt.Elem())
	case reflect.Chan:
		return "chan " + typeRegistrationName(rt.Elem())
	default:
		// structs, funcs, and interfaces are spelled by reflect; their element types are not qualified
		return rt.String()
	}
}
