package ftype_test

import (
	"fmt"
	"testing"

	"github.com/futura-platform/futura/ftype"
	"github.com/stretchr/testify/assert"
)

type someStruct struct {
	A int
	B string
}

func TestSealed(t *testing.T) {
	testForValue(t, 1)
	testForValue(t, "1")
	testForValue(t, []int{1, 2, 3})
	testForValue(t, []string{"1", "2", "3"})
	testForValue(t, []int{1, 2, 3})
	testForValue(t, []someStruct{{A: 1, B: "2"}, {A: 3, B: "4"}})
}

func testForValue[T any](t *testing.T, value T) {
	t.Run(fmt.Sprintf("sealed value for %T", value), func(t *testing.T) {
		sealedA := ftype.Seal(value)
		sealedB := ftype.Seal(value)
		assert.True(t, sealedA == sealedB)
	})
}
