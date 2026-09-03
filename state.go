package futura

import (
	"bytes"
	"context"
	"fmt"

	"github.com/futura-platform/futura/ftype"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/goroutinebind"
	"github.com/futura-platform/futura/moment"
	"github.com/futura-platform/futura/privateencoding"
)

// this isn't exported so that StateContainer can be comparable
type stateContainerImplementation[T comparable] struct {
	getValue func() T
	setState func(T)
}

type StateContainer[T comparable] interface {
	V() T
	Set(value T)
}

func (s stateContainerImplementation[T]) V() T {
	return s.getValue()
}

func (s stateContainerImplementation[T]) Set(value T) {
	s.setState(value)
}

func encodeState[T comparable](value T) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := privateencoding.NewEncoder[T](&buffer)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func decodeState[T comparable](data []byte) (T, error) {
	decoder := privateencoding.NewDecoder[T](bytes.NewReader(data))
	return decoder.Decode()
}

func stateWithInitialValue[T comparable](b FlowBuilder, initialValue T) StateContainer[T] {
	f := execution.MustFromContext(b)

	stateKey, err := Step(b, func(ctx context.Context, initialValue T) (string, error) {
		return fmt.Sprintf("%T-state[%s](%v)", initialValue, moment.CurrentIdentity(ctx), initialValue), nil
	}, initialValue, ftype.WithLabel("stateWithInitialValue"))
	if err != nil {
		// the key derivation has no error case, this should never happen
		panic(ftrerrors.InconsistentStateError(err))
	}

	// V and Set are callable from any goroutine, so they bind to the caller's goroutine on each call.
	read := func() (data []byte, value T) {
		data, ok := f.ReadBehind(goroutinebind.BindGoroutine(b), stateKey)
		if !ok {
			return nil, initialValue
		}
		value, err := decodeState[T](data)
		if err != nil {
			panic(err)
		}
		return data, value
	}

	return stateContainerImplementation[T]{
		getValue: func() T {
			_, value := read()
			return value
		},
		setState: func(newValue T) {
			current, value := read()
			if value == newValue {
				return
			}
			encoded, err := encodeState(newValue)
			if err != nil {
				panic(err)
			}
			// slow path comparison, do this to cover values like
			// NaN and pointers having funky equality rules before serialization
			if current != nil && bytes.Equal(current, encoded) {
				return
			}

			f.WriteBehind(stateKey, encoded)
		},
	}
}

// State is a function that allows a stateful value to be defined and updated within a flow.
// It's value will persist across replays of the flow.
// If setState is called with an updated value, it will update the state then immediately trigger a replay of the flow.
// Otherwise, it will do nothing.
func State[T comparable](b FlowBuilder, initialValue ...T) StateContainer[T] {
	switch len(initialValue) {
	case 0:
		var zero T
		return stateWithInitialValue(b, zero)
	case 1:
		return stateWithInitialValue(b, initialValue[0])
	default:
		panic(fmt.Sprintf("State can only be called with 1 initial value argument, got %d", len(initialValue)))
	}
}
