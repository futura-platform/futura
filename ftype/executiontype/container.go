package executiontype

import (
	"iter"

	"github.com/futura-platform/futura/ftype/datastore"
	"github.com/futura-platform/futura/moment"
)

type TransactionalContainer datastore.Transactional[Container, ReadOnlyContainer]

// Container is a container for the flow execution state.
// This allows for the flow execution state to be backed by any transactional storage mechanism.
// All values in the container are expected to only be accessed by a single thread of execution,
// so implementing an in-memory fast path is strongly recommended for optimal performance,
// alongside a durable storage layer for fault tolerance.
type Container interface {
	ReadOnlyContainer
	// call order
	SetCallOrderAt(index int, identity moment.Identity)
	AppendCallOrder(identity moment.Identity)

	// memo table
	SetMoment(identity moment.Identity, moment moment.Moment)
	DeleteMoment(identity moment.Identity)

	// durable state (essentially just a basic key-value store)
	StoreDurable(key string, value []byte) error
}

type ReadOnlyContainer interface {
	// call order
	CallOrderLength() int
	CallOrderAt(index int) moment.Identity

	// memo table
	HasMoment(identity moment.Identity) bool
	GetMoment(identity moment.Identity) (moment.Moment, bool)

	// all moments in the memo table that have not been deleted
	KnownMoments() iter.Seq[moment.Identity]

	// durable state
	LoadDurable(key string) ([]byte, bool, error)
}
