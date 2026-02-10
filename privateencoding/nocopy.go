package privateencoding

import "reflect"

// locker matches the method set used by the standard library to mark lock-like
// types (e.g. sync.Mutex, sync.RWMutex). We intentionally define it locally to
// avoid pulling in additional dependencies just for reflect.Type checks.
type locker interface {
	Lock()
	Unlock()
}

var lockerType = reflect.TypeOf((*locker)(nil)).Elem()

func isNoCopyStructType(t reflect.Type) bool {
	if t.Kind() != reflect.Struct {
		return false
	}
	// Most lock types implement the methods on the pointer receiver.
	return t.Implements(lockerType) || reflect.PointerTo(t).Implements(lockerType)
}
