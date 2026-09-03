package privateencoding

import "reflect"

// isNoCopyStructType reports whether t is one of the standard library's synchronization
// primitives (sync.Mutex, sync.RWMutex, sync.Once, ...). Their fields are runtime state, not
// logical state, so they are skipped. Only the primitive itself is skipped: a struct that embeds
// one still encodes its own fields.
func isNoCopyStructType(t reflect.Type) bool {
	return t.Kind() == reflect.Struct && t.PkgPath() == "sync"
}
