package moment

import (
	"fmt"
	"runtime"

	"github.com/futura-platform/futura/internal/donotcompare"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
)

// A Moment represents an Fn instance and its returned successful output at a specific point in time.
// This is separate from the actual Identifier of the specific point in time. That is represented by Identity.
type Moment struct {
	callableRef anyFn
	input       any
	output      any
	invalidated bool
}

type anyFn interface {
	runtimeFunc() *runtime.Func
	Label() string
}

func NewMoment[A comparable](callable anyFn, input A) *Moment {
	return &Moment{callableRef: callable, input: input}
}

// Validate validates the moment against the new input.
// Basically, if the input has changed, the moment is no longer valid.
// This will panic if it detects an impurity in the flow.
// againstFn must be a function, this should not change between replays for each moment.
func (m Moment) Validate(index int, againstFn anyFn, input any, identity Identity) (valid bool) {
	// check if the new fn is diverging from the existing moment's fn
	againstFnRef := againstFn.runtimeFunc()
	oldMomentFnRef := m.callableRef.runtimeFunc()
	if againstFnRef != oldMomentFnRef {
		panic(ftrerrors.InconsistentStateError(MomentFnChangeError{
			Index:          index,
			OldMomentFnRef: m.callableRef,
			NewMomentFnRef: againstFn,
			Identity:       identity,
		}))
	}
	return m.input == input && !m.invalidated
}

func (m Moment) Output() any {
	return m.output
}

func (m *Moment) SetOutput(output any) {
	m.output = output
}

func (m *Moment) Invalidate() {
	m.invalidated = true
}

// identities should always lead to the same fn. If they don't, this error is thrown.
// something like passing the moment fn function as a variable value that changes between replays to a step call would cause this.
type MomentFnChangeError struct {
	donotcompare.T
	Index                          int
	Identity                       Identity
	OldMomentFnRef, NewMomentFnRef anyFn
}

func (e MomentFnChangeError) Is(target error) bool {
	t, ok := target.(MomentFnChangeError)
	if !ok {
		return false
	}

	return e.Index == t.Index &&
		e.Identity == t.Identity &&
		e.OldMomentFnRef.runtimeFunc() == t.OldMomentFnRef.runtimeFunc() &&
		e.NewMomentFnRef.runtimeFunc() == t.NewMomentFnRef.runtimeFunc()
}

func (e MomentFnChangeError) Error() string {
	return fmt.Sprintf(
		// "func of the existing moment does not match the func of the current moment (moment[%d]): %s != %s (old != new)",
		"func of moment[%d] changed: %s != %s (old != new) @ %s",
		e.Index,
		e.OldMomentFnRef.Label(),
		e.NewMomentFnRef.Label(),
		e.Identity,
	)
}
