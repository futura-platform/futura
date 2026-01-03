package testutil

import "testing"

// this is a way to add a futura call frame that isnt in a _test.go file
func TransparentCall(fn func()) {
	if !testing.Testing() {
		panic("TransparentCall can only be called in a test")
	}
	fn()
}
