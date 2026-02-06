package futura

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"

	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/flow/replay"
	"github.com/futura-platform/futura/internal/step"
	"github.com/futura-platform/futura/privateencoding"
	"github.com/vmihailenco/msgpack/v5"
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

var stateContext = NewDurableHandle(
	"state",
	func() *map[string][]byte {
		return &map[string][]byte{}
	},
	func(data []byte) (*map[string][]byte, error) {
		var state map[string][]byte
		err := msgpack.Unmarshal(data, &state)
		return &state, err
	},
	func(data *map[string][]byte) ([]byte, error) {
		return msgpack.Marshal(*data)
	},
)

var (
	ErrStateNotFound = errors.New("state not found")
)

func stateWithInitialValue[T comparable](b FlowBuilder, initialValue T) StateContainer[T] {
	f := execution.MustFromContext(b)
	callstack, ok := replay.GetClosestReplayUserCallstack(0)
	if !ok {
		panic(ftrerrors.InconsistentStateError(step.ErrEvaledOutsideOfAFlowFunction))
	}
	idCall := callstack[0]
	stateKey := fmt.Sprintf(
		"%T-state[%s:%s:%d](%v)",
		initialValue,
		idCall.Function,
		idCall.File,
		idCall.Line,
		initialValue,
	)
	stateRef, persist := stateContext.Use(b)

	setValue := func(value T) bool {
		var buffer bytes.Buffer
		encoder := privateencoding.NewEncoder[T](&buffer)
		err := encoder.Encode(value)
		if err != nil {
			panic(err)
		}
		(*stateRef)[stateKey] = buffer.Bytes()
		return persist()
	}

	err := Effect(b, func(ctx context.Context, initialValue T) error {
		setValue(initialValue)
		return nil
	}, initialValue)
	if err != nil {
		// this should never happen
		panic(err)
	}

	return stateContainerImplementation[T]{
		getValue: func() T {
			data, ok := (*stateRef)[stateKey]
			if !ok {
				panic(ftrerrors.InconsistentStateError(fmt.Errorf("%w: %s", ErrStateNotFound, stateKey)))
			}
			decoder := privateencoding.NewDecoder[T](bytes.NewReader(data))
			value, err := decoder.Decode()
			if err != nil {
				panic(err)
			}

			return value
		},
		setState: func(value T) {
			if didChange := setValue(value); !didChange {
				return
			}
			f.RestartCurrentReplay(b, errors.New("state updated by setState"))
			f.SetNextFlags(func(flags *replay.Flags) {
				// state changes might cause the flow to change, so we don't want to panic in that case.
				flags.PanicOnMomentOrderChange = false
			})
			f.EvictUnseenCachedStates(b)
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
		t := reflect.TypeOf((*T)(nil)).Elem()
		return stateWithInitialValue(b, reflect.Zero(t).Interface().(T))
	case 1:
		return stateWithInitialValue(b, initialValue[0])
	default:
		panic(fmt.Sprintf("State can only be called with 1 initial value argument, got %d", len(initialValue)))
	}
}
