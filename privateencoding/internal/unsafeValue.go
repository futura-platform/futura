package privateencodinginternal

import (
	"reflect"
	"unsafe"
)

// unsafeValue returns a reflect.Value that has its flags stripped,
// so that even if it is unexported, it can be used as if it was.
// However, if it is exported, it will simply return the original value.
func UnsafeValue(v reflect.Value) reflect.Value {
	if v.CanInterface() {
		return v
	} else if !v.CanAddr() {
		panic("value is not addressable")
	}
	ptr := unsafe.Pointer(v.UnsafeAddr())
	return reflect.NewAt(v.Type(), ptr).Elem()
}
