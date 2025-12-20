package futura

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/flow/fcontext"
)

// this isn't exported so that StateContainer can be comparable
type stateContainerImplementation[T comparable] struct {
	value    T
	setState func(T)
}

type StateContainer[T comparable] interface {
	V() T
	Set(value T)
}

func (s stateContainerImplementation[T]) V() T {
	return s.value
}

func (s stateContainerImplementation[T]) Set(value T) {
	s.setState(value)
}

func stateWithInitialValue[T comparable](b FlowBuilder, initialValue T) StateContainer[T] {
	f := fcontext.MustFromContext(b)
	stateRef := refWithInitialValue(b, initialValue, ftype.WithLabel(fmt.Sprintf(
		"%T-state[%d](%v)",
		initialValue,
		f.SequenceIndex(),
		initialValue,
	)))
	return stateContainerImplementation[T]{
		value: *stateRef,
		setState: func(value T) {
			if value == *stateRef {
				return
			}
			*stateRef = value
			f.RestartCurrentReplay(b, errors.New("state updated by setState"))
			f.SetReplayFlags(func(flags *fcontext.ReplayFlags) {
				// state changes might cause the flow to change, so we don't want to panic in that case.
				flags.PanicOnMomentOrderChange = false
			})
			f.Rewind()
			f.EvictUnseenCachedStates(b)
		},
	}
}

// State is a function that allows a stateful value to be defined and updated within a flow.
// If setState is called with an updated value, it will update the state then immediately trigger a replay of the flow.
// Otherwise, it will do nothing.
func State[T comparable](b FlowBuilder, initialValue ...T) StateContainer[T] {
	switch len(initialValue) {
	case 0:
		t := reflect.TypeOf((*T)(nil)).Elem()
		return stateWithInitialValue(b, reflect.Zero(t).Interface().(T))
	case 1:
		return stateWithInitialValue(b, initialValue[0])
	default:
		panic(fmt.Sprintf("State can only be called with 1 initial value argument, got %d", len(initialValue)))
	}
}
