package moment_test

import (
	"bytes"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/futura-platform/futura/moment"
	"github.com/futura-platform/futura/privateencoding"
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

type cfg struct{ N int }

func TestValidateFallsBackToEncodedBytes(t *testing.T) {
	// == is the fast path. When it says the input changed, the encoded bytes decide,
	// so values that == cannot compare correctly still hit their memo.
	// The step registers its input type before recording; do the same here.
	privateencoding.Register[*cfg]()

	t.Run("a fresh pointer to an equal value is a hit", func(t *testing.T) {
		moment1 := moment.NewMoment(&cfg{1})
		moment1.SetValidOutput("out")
		assert.True(t, moment1.Validate(&cfg{1}))
		assert.False(t, moment1.Validate(&cfg{2}))
	})
	t.Run("NaN is a hit against NaN", func(t *testing.T) {
		moment1 := moment.NewMoment(math.NaN())
		moment1.SetValidOutput("out")
		assert.True(t, moment1.Validate(math.NaN()))
		assert.False(t, moment1.Validate(1.0))
	})
	t.Run("a value the encoder rejects is a miss", func(t *testing.T) {
		type withChan struct{ C chan int }
		c := make(chan int)
		moment1 := moment.NewMoment(withChan{c})
		moment1.SetValidOutput("out")
		assert.True(t, moment1.Validate(withChan{c}), "== still answers on the fast path")
		assert.False(t, moment1.Validate(withChan{make(chan int)}), "and the fallback cannot vouch for it")
	})
	t.Run("a cyclic value is a miss, not a crash", func(t *testing.T) {
		type node struct {
			V    int
			Next *node
		}
		n := &node{V: 1}
		n.Next = n
		moment1 := moment.NewMoment(n)
		moment1.SetValidOutput("out")
		m := &node{V: 1}
		m.Next = m
		assert.False(t, moment1.Validate(m))
	})
	t.Run("the snapshot survives the container's own encoding of the moment", func(t *testing.T) {
		// A real backend serializes the whole moment and decodes it on the next machine. The fresh
		// input there is a new pointer, and it must still match the snapshot taken here.
		moment1 := moment.NewMoment(&cfg{1})
		moment1.SetValidOutput("out")
		var buf bytes.Buffer
		assert.NoError(t, privateencoding.NewEncoder[moment.Moment](&buf).Encode(*moment1))
		decoded, err := privateencoding.NewDecoder[moment.Moment](bytes.NewReader(buf.Bytes())).Decode()
		assert.NoError(t, err)
		assert.True(t, decoded.Validate(&cfg{1}))
		assert.False(t, decoded.Validate(&cfg{2}))
	})
	t.Run("the fallback compares against the input as recorded, not as it is now", func(t *testing.T) {
		recorded := &cfg{1}
		moment1 := moment.NewMoment(recorded)
		moment1.SetValidOutput("out")
		recorded.N = 2 // mutated after the fact
		assert.True(t, moment1.Validate(&cfg{1}), "the moment was recorded for N=1")
		assert.False(t, moment1.Validate(&cfg{2}))
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
