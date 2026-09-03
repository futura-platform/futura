package futura

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
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

	// Set is callable from any goroutine, so the value is loaded once and then guarded.
	var mu sync.Mutex
	var loaded bool
	var value T
	load := func() T {
		mu.Lock()
		defer mu.Unlock()
		if loaded {
			return value
		}
		data, ok := f.LoadDurable(goroutinebind.BindGoroutine(b), stateKey)
		if ok {
			if value, err = decodeState[T](data); err != nil {
				panic(err)
			}
		} else {
			value = initialValue
		}
		loaded = true
		return value
	}

	return stateContainerImplementation[T]{
		getValue: load,
		setState: func(newValue T) {
			if load() == newValue {
				return
			}
			encoded, err := encodeState(newValue)
			if err != nil {
				panic(err)
			}

			// The new value is only staged in memory here. It is committed durably, together with the
			// sequence invalidation, when the next replay starts. This allows for multiple state changes to happen atomically, ensuring durability.
			f.InvalidateSequence(
				func() {
					mu.Lock()
					defer mu.Unlock()
					value = newValue
				},
				func(tx executiontype.Container) {
					if err := tx.StoreDurable(execution.GenericDurableKey(stateKey), encoded); err != nil {
						panic(err)
					}
				},
			)
			f.RestartCurrentReplay(errors.New("state updated by setState"))
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
		t := reflect.TypeFor[T]()
		return stateWithInitialValue(b, reflect.Zero(t).Interface().(T))
	case 1:
		return stateWithInitialValue(b, initialValue[0])
	default:
		panic(fmt.Sprintf("State can only be called with 1 initial value argument, got %d", len(initialValue)))
	}
}
