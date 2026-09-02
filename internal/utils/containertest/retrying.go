package containertest

import (
	"context"
	"iter"

	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/futura-platform/futura/moment"
)

// Retrying runs every transaction closure attempts times, the way a transactional backend
// does when it retries on conflict. Only the last attempt's writes are committed.
type Retrying struct {
	inner    executiontype.TransactionalContainer
	attempts int

	// StaleView, if set, is applied to the view of every discarded attempt,
	// to model the conflicting state that caused the retry.
	StaleView func(tx executiontype.Container)
	// Calls counts closure invocations across all transactions.
	Calls int
}

var _ executiontype.TransactionalContainer = &Retrying{}

func NewRetrying(inner executiontype.TransactionalContainer, attempts int) *Retrying {
	if attempts < 1 {
		panic("a transaction needs at least one attempt")
	}
	return &Retrying{inner: inner, attempts: attempts}
}

func (r *Retrying) Transact(ctx context.Context, fn func(ctx context.Context, tx executiontype.Container) error) error {
	return r.inner.Transact(ctx, func(ctx context.Context, tx executiontype.Container) error {
		if err := r.discard(ctx, tx, func(ctx context.Context, view *overlay) error { return fn(ctx, view) }); err != nil {
			return err
		}
		r.Calls++
		return fn(ctx, tx)
	})
}

func (r *Retrying) ReadTransact(ctx context.Context, fn func(ctx context.Context, tx executiontype.ReadOnlyContainer) error) error {
	return r.inner.ReadTransact(ctx, func(ctx context.Context, tx executiontype.ReadOnlyContainer) error {
		if err := r.discard(ctx, tx, func(ctx context.Context, view *overlay) error { return fn(ctx, view) }); err != nil {
			return err
		}
		r.Calls++
		return fn(ctx, tx)
	})
}

// discard runs fn against a throwaway view of tx for every attempt but the last.
func (r *Retrying) discard(ctx context.Context, tx executiontype.ReadOnlyContainer, fn func(ctx context.Context, view *overlay) error) error {
	for range r.attempts - 1 {
		r.Calls++
		view := newOverlay(tx)
		if r.StaleView != nil {
			r.StaleView(view)
		}
		if err := fn(ctx, view); err != nil {
			return err
		}
	}
	return nil
}

// overlay is a view of base whose writes never reach it.
type overlay struct {
	base      executiontype.ReadOnlyContainer
	callOrder []moment.Identity
	moments   map[moment.Identity]moment.Moment
	deleted   map[moment.Identity]bool
	durable   map[string][]byte
}

var _ executiontype.Container = &overlay{}

func newOverlay(base executiontype.ReadOnlyContainer) *overlay {
	callOrder := make([]moment.Identity, base.CallOrderLength())
	for i := range callOrder {
		callOrder[i] = base.CallOrderAt(i)
	}
	return &overlay{
		base:      base,
		callOrder: callOrder,
		moments:   map[moment.Identity]moment.Moment{},
		deleted:   map[moment.Identity]bool{},
		durable:   map[string][]byte{},
	}
}

func (o *overlay) CallOrderLength() int                               { return len(o.callOrder) }
func (o *overlay) CallOrderAt(index int) moment.Identity              { return o.callOrder[index] }
func (o *overlay) SetCallOrderAt(index int, identity moment.Identity) { o.callOrder[index] = identity }
func (o *overlay) AppendCallOrder(identity moment.Identity) {
	o.callOrder = append(o.callOrder, identity)
}
func (o *overlay) TruncateCallOrderAt(index int) {
	o.callOrder = o.callOrder[:min(index+1, len(o.callOrder))]
}

func (o *overlay) HasMoment(identity moment.Identity) bool {
	_, ok := o.GetMoment(identity)
	return ok
}
func (o *overlay) GetMoment(identity moment.Identity) (moment.Moment, bool) {
	if m, ok := o.moments[identity]; ok {
		return m, true
	}
	if o.deleted[identity] {
		return moment.Moment{}, false
	}
	return o.base.GetMoment(identity)
}
func (o *overlay) SetMoment(identity moment.Identity, m moment.Moment) {
	o.moments[identity] = m
	delete(o.deleted, identity)
}
func (o *overlay) DeleteMoment(identity moment.Identity) {
	delete(o.moments, identity)
	o.deleted[identity] = true
}
func (o *overlay) KnownMoments() iter.Seq[moment.Identity] {
	return func(yield func(moment.Identity) bool) {
		for identity := range o.base.KnownMoments() {
			if !o.deleted[identity] && !yield(identity) {
				return
			}
		}
		for identity := range o.moments {
			if !o.base.HasMoment(identity) && !yield(identity) {
				return
			}
		}
	}
}

func (o *overlay) LoadDurable(key string) ([]byte, bool, error) {
	if value, ok := o.durable[key]; ok {
		return value, true, nil
	}
	return o.base.LoadDurable(key)
}
func (o *overlay) StoreDurable(key string, value []byte) error {
	o.durable[key] = value
	return nil
}
