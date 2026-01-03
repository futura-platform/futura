package testutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func PanicsWithErrorIs(t *testing.T, expectedError error, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		err, ok := r.(error)
		if !ok {
			t.Fatalf("expected panic with error, got %+v", r)
		}
		assert.ErrorIs(t, err, expectedError)
	}()
	fn()
}
