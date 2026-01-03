package testutil

import (
	"reflect"
	"testing"
)

// IsTestParallel uses reflection to check if t.Parallel() was called.
// This relies on Go internals and may break in future Go versions.
func IsTestParallel(t *testing.T) bool {
	v := reflect.ValueOf(t).Elem()
	f := v.FieldByName("isParallel")
	if !f.IsValid() {
		return false
	}
	return f.Bool()
}
