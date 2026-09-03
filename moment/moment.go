package moment

import (
	"bytes"

	"github.com/futura-platform/futura/privateencoding"
	"github.com/samber/mo"
)

// A Moment represents an Fn instance and its returned successful output at a specific point in time.
// This is separate from the actual Identifier of the specific point in time. That is represented by Identity.
type Moment struct {
	input any
	// record the encoded format for comparison, so we don't have to re encode it every time
	encodedInput []byte
	output       mo.Option[any]
}

func NewMoment[A comparable](input A) *Moment {
	return &Moment{input: input, encodedInput: encodeInput(input)}
}

// Validate reports whether the moment can be reused for the current replay.
// A moment is reusable when the input is unchanged and it still has a valid output.
func (m Moment) Validate(input any) (valid bool) {
	if !m.output.IsSome() {
		return false
	}
	if m.input == input {
		return true
	}
	return m.encodedInput != nil && bytes.Equal(m.encodedInput, encodeInput(input))
}

// encodeInput encodes an input for comparison, or returns nil if it cannot be encoded.
func encodeInput(input any) []byte {
	var buf bytes.Buffer
	if err := privateencoding.NewEncoder[any](&buf).Encode(input); err != nil {
		return nil
	}
	return buf.Bytes()
}

func (m Moment) Output() mo.Option[any] {
	return m.output
}

func (m *Moment) SetValidOutput(output any) {
	m.output = mo.Some(output)
}

func (m *Moment) Invalidate() {
	m.output = mo.None[any]()
}
