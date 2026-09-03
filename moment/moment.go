package moment

import "github.com/samber/mo"

// A Moment represents an Fn instance and its returned successful output at a specific point in time.
// This is separate from the actual Identifier of the specific point in time. That is represented by Identity.
type Moment struct {
	input  any
	output mo.Option[any]
}

func NewMoment[A comparable](input A) *Moment {
	return &Moment{input: input}
}

// Validate reports whether the moment can be reused for the current replay.
// A moment is reusable when the input is unchanged and it still has a valid output.
func (m Moment) Validate(input any) (valid bool) {
	return m.input == input && m.output.IsSome()
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
