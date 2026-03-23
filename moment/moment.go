package moment

import (
	"fmt"
	"runtime"

	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/samber/mo"
)

// A Moment represents an Fn instance and its returned successful output at a specific point in time.
// This is separate from the actual Identifier of the specific point in time. That is represented by Identity.
type Moment struct {
	// callableName is the runtime symbol name used for replay validation.
	// This is intentionally distinct from any user-facing label attached to an Fn.
	callableName string
	input        any
	output       mo.Option[any]
}

type anyFn interface {
	runtimeFunc() *runtime.Func
	Label() string
}

// NewMoment stores the callable's runtime identity so replays can detect when the flow diverges.
func NewMoment[A comparable](callable anyFn, input A) *Moment {
	return &Moment{callableName: callable.runtimeFunc().Name(), input: input}
}

// Validate reports whether the moment can be reused for the current replay.
// A moment is reusable when the input is unchanged, it still has a valid output,
// and the runtime function identity matches the function recorded when the moment was created.
// It panics if the runtime function identity changed, because that indicates an impure flow.
func (m Moment) Validate(index int, currentFn anyFn, input any, identity Identity) (valid bool) {
	currentFnName := currentFn.runtimeFunc().Name()
	recordedFnName := m.callableName
	if currentFnName != recordedFnName {
		panic(ftrerrors.InconsistentStateError(MomentFnChangeError{
			Index:           index,
			OldMomentFnName: recordedFnName,
			NewMomentFnName: currentFnName,
			Identity:        identity,
		}))
	}
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

// MomentFnChangeError reports that an identity led to a different function across replays.
// This can happen if a moment function is passed through changing variables or control flow.
type MomentFnChangeError struct {
	Index                            int
	Identity                         Identity
	OldMomentFnName, NewMomentFnName string
}

func (e MomentFnChangeError) Error() string {
	return fmt.Sprintf(
		"func of moment[%d] changed: %s != %s (old != new) @ %s",
		e.Index,
		e.OldMomentFnName,
		e.NewMomentFnName,
		e.Identity,
	)
}
