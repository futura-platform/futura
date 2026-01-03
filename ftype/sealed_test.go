package ftype

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type someStruct struct {
	A int
	B string
}

func TestSealValidValues(t *testing.T) {
	testForValue(t, 1)
	testForValue(t, "1")
	testForValue(t, []int{1, 2, 3})
	testForValue(t, []string{"1", "2", "3"})
	testForValue(t, []int{1, 2, 3})
	testForValue(t, []someStruct{{A: 1, B: "2"}, {A: 3, B: "4"}})
}

func TestSealInvalidValues(t *testing.T) {
	assert.Panics(t, func() { Seal(func() {}) })
	assert.Panics(t, func() { Seal(make(chan int)) })
}
func TestUnsealInvalidValue(t *testing.T) {
	invalidSealedInt := sealedWithString[int]{
		comparableSerialized: "some too long value for an int",
	}
	assert.Panics(t, func() { invalidSealedInt.V() })

	invalidSealedString := sealedWithString[string]{
		comparableSerialized: "some invalid serialized string",
	}
	assert.Panics(t, func() { invalidSealedString.V() })
}

func testForValue[T any](t *testing.T, value T) {
	t.Run(fmt.Sprintf("sealed value for %T", value), func(t *testing.T) {
		sealedA := Seal(value)
		sealedB := Seal(value)
		// assert comparability
		assert.True(t, sealedA == sealedB)
		// assert deep equality
		assert.Equal(t, value, sealedA.V())
	})
}
