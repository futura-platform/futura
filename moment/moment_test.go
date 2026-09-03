package moment_test

import (
	"math/rand/v2"
	"testing"

	"github.com/futura-platform/futura/moment"
	"github.com/stretchr/testify/assert"
)

func TestValidate(t *testing.T) {
	t.Run("valid case", func(t *testing.T) {
		moment1 := moment.NewMoment(1)
		moment1.SetValidOutput("something")
		assert.True(t, moment1.Validate(1))
	})
	t.Run("invalid cases", func(t *testing.T) {
		t.Run("input changed", func(t *testing.T) {
			moment1 := moment.NewMoment(1)
			assert.False(t, moment1.Validate(2))
		})
		t.Run("moment was explicitly invalidated, otherwise valid", func(t *testing.T) {
			moment1 := moment.NewMoment(1)
			moment1.SetValidOutput("something")
			assert.True(t, moment1.Validate(1))

			moment1.Invalidate()
			assert.False(t, moment1.Validate(1))

			t.Run("setting the output to a different value makes it valid again", func(t *testing.T) {
				moment1.SetValidOutput(2)
				assert.True(t, moment1.Validate(1))
			})
		})
	})
}

func TestOutput(t *testing.T) {
	moment1 := moment.NewMoment(1)
	output := rand.Int()
	moment1.SetValidOutput(output)
	outFromMoment, ok := moment1.Output().Get()
	assert.True(t, ok)
	assert.Equal(t, output, outFromMoment)
}
