package containertest

import (
	"context"
	"slices"
	"testing"

	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/futura-platform/futura/moment"
	"github.com/stretchr/testify/assert"
)

func TestStrict(t *testing.T) {
	t.Run("only the last attempt is committed", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		r := NewStrict(c)

		attempt := 0
		err := r.Transact(t.Context(), func(_ context.Context, tx executiontype.Container) error {
			attempt++
			return tx.StoreDurable("attempt", []byte{byte(attempt)})
		})
		assert.NoError(t, err)
		assert.Equal(t, Attempts, r.Calls)

		value, ok, err := c.LoadDurable("attempt")
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte{3}, value)
	})
	t.Run("every attempt reads the committed state, and never another attempt's writes", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		assert.NoError(t, c.StoreDurable("committed", []byte{1}))
		r := NewStrict(c)

		err := r.Transact(t.Context(), func(_ context.Context, tx executiontype.Container) error {
			_, ok, err := tx.LoadDurable("committed")
			assert.NoError(t, err)
			assert.True(t, ok)

			_, ok, err = tx.LoadDurable("mine")
			assert.NoError(t, err)
			assert.False(t, ok, "a previous attempt's write leaked into this one")
			return tx.StoreDurable("mine", []byte{1})
		})
		assert.NoError(t, err)
	})
	t.Run("the stale view is only shown to discarded attempts", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		r := NewStrict(c)
		r.StaleView = func(tx executiontype.Container) {
			assert.NoError(t, tx.StoreDurable("stale", []byte{1}))
		}

		var seen []bool
		err := r.Transact(t.Context(), func(_ context.Context, tx executiontype.Container) error {
			_, ok, err := tx.LoadDurable("stale")
			assert.NoError(t, err)
			seen = append(seen, ok)
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, []bool{true, true, false}, seen)
	})
	t.Run("call order and memo edits are discarded with their attempt", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		committed := moment.NewIdentity(t.Context(), moment.Callpath{{File: "committed"}})
		c.AppendCallOrder(committed)
		c.SetMoment(committed, moment.Moment{})
		r := NewStrict(c)

		err := r.Transact(t.Context(), func(_ context.Context, tx executiontype.Container) error {
			assert.Equal(t, 1, tx.CallOrderLength(), "a previous attempt's append leaked into this one")
			assert.True(t, tx.HasMoment(committed), "a previous attempt's delete leaked into this one")

			mine := moment.NewIdentity(t.Context(), moment.Callpath{{File: "mine"}})
			tx.AppendCallOrder(mine)
			tx.SetMoment(mine, moment.Moment{})
			tx.DeleteMoment(committed)
			assert.Equal(t, 2, tx.CallOrderLength())
			assert.False(t, tx.HasMoment(committed))
			assert.ElementsMatch(t, []moment.Identity{mine}, slices.Collect(tx.KnownMoments()))
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, c.CallOrderLength())
		assert.False(t, c.HasMoment(committed))
	})
	t.Run("read transactions are retried too", func(t *testing.T) {
		r := NewStrict(executiontype.NewInMemoryContainer())
		calls := 0
		err := r.ReadTransact(t.Context(), func(context.Context, executiontype.ReadOnlyContainer) error {
			calls++
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, Attempts, calls)
	})
	t.Run("a transaction on a done context is refused without running", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		r := NewStrict(executiontype.NewInMemoryContainer())

		calls := 0
		err := r.Transact(ctx, func(context.Context, executiontype.Container) error {
			calls++
			return nil
		})
		assert.ErrorIs(t, err, context.Canceled)
		err = r.ReadTransact(ctx, func(context.Context, executiontype.ReadOnlyContainer) error {
			calls++
			return nil
		})
		assert.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, 0, calls)
		assert.Equal(t, 0, r.Calls)
	})
}
