package executiontype

import (
	"context"
	"iter"

	"github.com/futura-platform/futura/moment"
)

type InMemoryContainer struct {
	seenStates map[moment.Identity]bool
	State
}

// LoadDurable implements Container.
func (i *InMemoryContainer) LoadDurable(key string) ([]byte, bool, error) {
	value, ok := i.durableState[key]
	return value, ok, nil
}

// StoreDurable implements Container.
func (i *InMemoryContainer) StoreDurable(key string, value []byte) error {
	i.durableState[key] = value
	return nil
}

func NewInMemoryContainer() *InMemoryContainer {
	return &InMemoryContainer{
		seenStates: make(map[moment.Identity]bool),
		State:      NewState(),
	}
}

var _ TransactionalContainer = &InMemoryContainer{}

// Transact implements TransactionalContainer.
func (i *InMemoryContainer) Transact(ctx context.Context, fn func(ctx context.Context, tx Container) error) error {
	return fn(ctx, i)
}

// since this is in memory, we can just use the same transaction function
func (i *InMemoryContainer) ReadTransact(ctx context.Context, fn func(ctx context.Context, tx ReadOnlyContainer) error) error {
	return fn(ctx, i)
}

var _ Container = &InMemoryContainer{}

// CallOrderAt implements Container.
func (i *InMemoryContainer) CallOrderAt(index int) moment.Identity {
	return i.callOrder[index]
}

// CallOrderLength implements Container.
func (i *InMemoryContainer) CallOrderLength() int {
	return len(i.callOrder)
}

// DeleteMoment implements Container.
func (i *InMemoryContainer) DeleteMoment(identity moment.Identity) {
	delete(i.memoTable, identity)
}

// KnownMoments implements Container.
func (i *InMemoryContainer) KnownMoments() iter.Seq[moment.Identity] {
	return func(yield func(moment.Identity) bool) {
		for identity := range i.memoTable {
			if !yield(identity) {
				return
			}
		}
	}
}

// AppendCallOrder implements Container.
func (i *InMemoryContainer) AppendCallOrder(identity moment.Identity) {
	i.callOrder = append(i.callOrder, identity)
}

// SetCallOrderAt implements Container.
func (i *InMemoryContainer) SetCallOrderAt(index int, identity moment.Identity) {
	i.callOrder[index] = identity
}

// SetMoment implements Container.
func (i *InMemoryContainer) SetMoment(identity moment.Identity, moment moment.Moment) {
	i.memoTable[identity] = moment
}

// GetMoment implements Container.
func (i *InMemoryContainer) GetMoment(identity moment.Identity) (moment.Moment, bool) {
	moment, ok := i.memoTable[identity]
	return moment, ok
}

// HasMoment implements Container.
func (i *InMemoryContainer) HasMoment(identity moment.Identity) bool {
	_, ok := i.memoTable[identity]
	return ok
}
