package privateencoding_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// privateencoding promises comparable bytes, so equal maps must serialize
// identically despite Go's randomized map iteration order.
func TestEncoder_MapEncodingIsDeterministic(t *testing.T) {
	reference := newMapEncodingFixture(false, 0)
	want := append([]byte(nil), encodeValue(t, reference)...)

	for i := range 512 {
		candidate := newMapEncodingFixture(i%2 == 1, i+1)
		got := encodeValue(t, candidate)

		require.Truef(
			t,
			bytes.Equal(want, got),
			"equal maps encoded differently on iteration %d\nreference: %x\ncandidate: %x",
			i+1,
			want,
			got,
		)
	}
}

func newMapEncodingFixture(reverse bool, churn int) map[string]int {
	values := make(map[string]int)

	insert := func(i int) {
		values[fmt.Sprintf("key-%02d", i)] = i * i
	}

	if reverse {
		for i := 31; i >= 0; i-- {
			insert(i)
		}
	} else {
		for i := range 32 {
			insert(i)
		}
	}

	// Add and remove extra keys so logically equal maps can still have
	// different internal histories.
	for i := range churn % 11 {
		dummyKey := fmt.Sprintf("dummy-%02d", i)
		values[dummyKey] = -i
		delete(values, dummyKey)
	}

	return values
}
