package replay

import (
	"reflect"
	"runtime"
)

// runtimeFunctionName returns the name a function is reported under in a runtime.Frame.
// Generic functions are reported with their type parameters elided, so any instantiation
// resolves to the same name.
func runtimeFunctionName(fn any) string {
	return runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
}
