package privateencoding_test

import (
	"reflect"
	"testing"

	"github.com/futura-platform/futura/privateencoding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegistry_DistinctTypesWithTheSameNameAreAnError(t *testing.T) {
	// reflection cannot tell two function-local `type args struct{...}` apart: same package, same
	// name. The name is the tag in the stored bytes, so the second one cannot be tagged at all; that
	// is reported where the type is first used, not silently mis-decoded later
	mk := func() reflect.Type {
		type args struct{ N int }
		return reflect.TypeFor[args]()
	}
	mk2 := func() reflect.Type {
		type args struct{ S string }
		return reflect.TypeFor[args]()
	}
	first, second := mk(), mk2()
	require.NotEqual(t, first, second)
	privateencoding.RegisterType(first)
	assert.PanicsWithValue(t,
		"privateencoding: two distinct types share the registration name \"github.com/futura-platform/futura/privateencoding_test.args\" "+
			"(privateencoding_test.args and privateencoding_test.args); "+
			"a type declared inside a function is known only by its package and name, so give them distinct names",
		func() { privateencoding.RegisterType(second) })
}
