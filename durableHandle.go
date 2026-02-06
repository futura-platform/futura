package futura

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"github.com/futura-platform/futura/ftype"
)

type durableKey string

type DurableHandle[T any] struct {
	id  int32
	key durableKey

	constructor func() *T
	unmarshal   func([]byte) (*T, error)
	marshal     func(*T) ([]byte, error)
}

var handleSequence atomic.Int32

// NewDurableHandle creates a new durable handle.
// This is designed to be used as a singleton, at 1 or more scopes above the Flow Execute call.
// The key is used to identify the value within the task's execution container.
// The same DurableHandle key may only be used at most once per flow.
// Values are stored in the consuming flow's execution container.
func NewDurableHandle[T any](
	key string,
	constructor func() *T,
	unmarshal func([]byte) (*T, error),
	marshal func(*T) ([]byte, error),
) *DurableHandle[T] {
	return &DurableHandle[T]{
		id:          handleSequence.Add(1),
		key:         durableKey(key),
		constructor: constructor,
		unmarshal:   unmarshal,
		marshal:     marshal,
	}
}

type durableResolver[T any] struct {
	handleId int32
	resolve  func(b FlowBuilder) (value *T, remotechecksum uint64)
}

var (
	ErrDurableResolverAlreadyProvided = errors.New("durable resolver already provided")
)

// Provide returns a FlowLoopOption that will provide the durable resolver to the flow.
func (d *DurableHandle[T]) Provide() ftype.FlowLoopOption {
	return func(ctx context.Context) context.Context {
		// first check if the context already has a value for this key
		if _, ok := ctx.Value(d.key).(durableResolver[T]); ok {
			panic(fmt.Errorf("%w: %s", ErrDurableResolverAlreadyProvided, d.key))
		}

		resolver := durableResolver[T]{
			handleId: d.id,
			resolve: func(b FlowBuilder) (*T, uint64) {
				serialized, ok := b.execution.LoadDurable(b, string(d.key))
				if !ok {
					return d.constructor(), 0 // remote checksum should never match if no value is found, since there is no remote value
				}
				currentValue, err := d.unmarshal(serialized)
				if err != nil {
					panic(err)
				}
				return currentValue, xxhash.Sum64(serialized)
			},
		}
		return context.WithValue(ctx, d.key, resolver)
	}
}

var (
	ErrDurableResolverNotFound = errors.New("durable resolver not found")
	ErrDurableResolverMismatch = errors.New("durable resolver mismatch")
)

// Use returns a reference to the durable value, and a function to persist the changes to the durable value.
// Persist should ALWAYS be called at some point after the ref is mutated.
// Persist will return true if the value was changed, and false if it was not.
// If persist returns false, that also implies that the StoreDurable call was skipped.
// Not doing so can cause the changes to be lost in the event of a failure.
// This function will attempt to load the value from the execution container,
// and if it fails, it will call the constructor to create a new value. (via the durableResolver).
func (d *DurableHandle[T]) Use(b FlowBuilder) (ref *T, persist func() (didChange bool)) {
	// first check if the builder context has a value for this key
	r, ok := b.Value(d.key).(durableResolver[T])
	if !ok {
		panic(fmt.Errorf("%w: %s", ErrDurableResolverNotFound, d.key))
	} else if r.handleId != d.id {
		panic(fmt.Errorf("%w: %s, expected %d, got %d", ErrDurableResolverMismatch, d.key, d.id, r.handleId))
	}

	ref, remoteChecksum := r.resolve(b)
	var persistMu sync.Mutex
	return ref, func() bool {
		persistMu.Lock()
		defer persistMu.Unlock()

		serialized, err := d.marshal(ref)
		if err != nil {
			panic(err)
		}

		// don't call store if the value hasn't changed
		localChecksum := xxhash.Sum64(serialized)
		didChange := localChecksum != remoteChecksum
		if didChange {
			b.execution.StoreDurable(b, string(d.key), serialized)
			// update remote checksum so repeated persist calls are idempotent
			remoteChecksum = localChecksum
		}
		return didChange
	}
}
