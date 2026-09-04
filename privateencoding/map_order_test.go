package privateencoding_test

import (
	"bytes"
	"math"
	"testing"
)

// Map entries are emitted in an order that depends only on their contents, so that a map encodes to the
// same bytes on every call. Sorting by key alone is not enough: different keys can encode identically
// (pointers encode as their pointee; every NaN encodes the same), and those entries would otherwise stay
// in Go's randomized iteration order.
func TestEncoder_MapEntryOrderIsDeterministic(t *testing.T) {
	same := func(t *testing.T, encode func() []byte) {
		t.Helper()
		want := encode()
		for i := range 200 {
			if got := encode(); !bytes.Equal(want, got) {
				t.Fatalf("iteration %d encoded differently:\n%x\n%x", i, want, got)
			}
		}
	}
	t.Run("pointer keys with equal pointees", func(t *testing.T) {
		a, b, c := 1, 1, 1
		m := map[*int]string{&a: "x", &b: "y", &c: "z"}
		same(t, func() []byte { return encodeValue(t, m) })
	})
	t.Run("NaN keys", func(t *testing.T) {
		m := map[float64]int{}
		m[math.NaN()] = 1
		m[math.NaN()] = 2
		m[math.NaN()] = 3
		same(t, func() []byte { return encodeValue(t, m) })
	})
	t.Run("distinct keys are unaffected", func(t *testing.T) {
		m := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}
		same(t, func() []byte { return encodeValue(t, m) })
	})
}
