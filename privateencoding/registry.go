package privateencoding

import (
	"fmt"
	"reflect"
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

func RegisterType(rt reflect.Type) {
	if rt.Kind() == reflect.Interface {
		panic("interface types cannot be registered")
	}
	registerNamedType(rt, typeRegistrationName(rt))
}

func registerNamedType(rt reflect.Type, name string) {
	registerMu.Lock()
	defer registerMu.Unlock()

	if existingName, ok := registeredTypeNameByType.Load(rt); ok {
		if existingName == name {
			return
		}
		panic(fmt.Sprintf(
			"privateencoding: type %s already registered as %q (got %q)",
			rt.String(),
			existingName,
			name,
		))
	}
	if existingType, ok := registeredTypeByName.Load(name); ok {
		if existingType == rt {
			return
		}
		panic(fmt.Sprintf(
			"privateencoding: type name %q already registered for %s (got %s)",
			name,
			existingType.String(),
			rt.String(),
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

// (logic copied from gob package)
// typeRegistrationName mirrors the standard library Register name derivation.
func typeRegistrationName(rt reflect.Type) string {
	// Default to printed representation for unnamed types.
	name := rt.String()

	// For named types (or pointers to them), qualify with import path.
	// The pointer behavior intentionally mirrors existing compatibility behavior.
	star := ""
	if rt.Name() == "" {
		if pt := rt; pt.Kind() == reflect.Pointer {
			star = "*"
			rt = pt
		}
	}
	if rt.Name() != "" {
		if rt.PkgPath() == "" {
			name = star + rt.Name()
		} else {
			name = star + rt.PkgPath() + "." + rt.Name()
		}
	}

	return name
}
