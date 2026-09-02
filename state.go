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

// stateValues holds every State's encoded value for a flow. Set is callable from any goroutine and
// the loop marshals the map when it commits, so all access goes through the mutex.
type stateValues struct {
	mu     sync.RWMutex
	values map[string][]byte
}

func (s *stateValues) get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.values[key]
	return value, ok
}

func (s *stateValues) set(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[key] = value
}

var stateContext = NewDurableHandle(
	"state",
	func() *stateValues {
		return &stateValues{values: map[string][]byte{}}
	},
	func(data []byte) (*stateValues, error) {
		decoder := privateencoding.NewDecoder[map[string][]byte](bytes.NewReader(data))
		values, err := decoder.Decode()
		if err != nil {
			return nil, err
		}
		return &stateValues{values: values}, nil
	},
	func(s *stateValues) ([]byte, error) {
		s.mu.RLock()
		defer s.mu.RUnlock()
		var buffer bytes.Buffer
		encoder := privateencoding.NewEncoder[map[string][]byte](&buffer)
		if err := encoder.Encode(s.values); err != nil {
			return nil, err
		}
		return buffer.Bytes(), nil
	},
	nil,
)

var (
	ErrStateNotFound = errors.New("state not found")
)

func stateWithInitialValue[T comparable](b FlowBuilder, initialValue T) StateContainer[T] {
	f := execution.MustFromContext(b)
	values, persist := stateContext.Use(b)

	encode := func(value T) ([]byte, error) {
		var buffer bytes.Buffer
		encoder := privateencoding.NewEncoder[T](&buffer)
		err := encoder.Encode(value)
		if err != nil {
			return nil, err
		}
		return buffer.Bytes(), nil
	}

	stage := func(stateKey string, value []byte) {
		values.set(stateKey, value)
	}

	stateKey, err := Step(b, func(ctx context.Context, initialValue T) (string, error) {
		stateKey := fmt.Sprintf("%T-state[%s](%v)", initialValue, moment.CurrentIdentity(ctx), initialValue)
		value, err := encode(initialValue)
		if err != nil {
			return "", err
		}
		stage(stateKey, value)
		persist()
		return stateKey, nil
	}, initialValue, ftype.WithLabel("stateWithInitialValue"))
	if err != nil {
		// the seed effect has no error case, this should never happen
		panic(ftrerrors.InconsistentStateError(err))
	}

	return stateContainerImplementation[T]{
		getValue: func() T {
			data, ok := values.get(stateKey)
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
			encoded, err := encode(value)
			if err != nil {
				panic(err)
			}
			if current, _ := values.get(stateKey); bytes.Equal(current, encoded) {
				return
			}

			b = b.WithContext(
				// setState is callable anywhere, so we need to temporarily bind to the current goroutine to allow the replay to restart.
				goroutinebind.BindGoroutine(b),
			)

			// The new value is only staged in memory here. It is committed durably, together with the
			// sequence invalidation, when the loop handles the restart. This allows for multiple state changes to happen atomically, ensuring durability.
			stage(stateKey, encoded)
			f.InvalidateSequence(func(tx executiontype.Container) { stateContext.WriteTo(b, tx) })
			f.RestartCurrentReplay(b, errors.New("state updated by setState"))
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
