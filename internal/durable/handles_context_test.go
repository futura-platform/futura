package durable_test

import (
	"testing"

	"github.com/futura-platform/futura/internal/durable"
	"github.com/futura-platform/futura/internal/utils/testutil"
	"github.com/stretchr/testify/assert"
)

func TestWithHandlesCache(t *testing.T) {
	ctx := t.Context()
	t.Run("should return false if handles don't already exist", func(t *testing.T) {
		handles, ok := durable.GetHandles(ctx)
		assert.False(t, ok)
		assert.Nil(t, handles)
	})
	t.Run("should create new handles without panicking if they don't already exist", func(t *testing.T) {
		ctx = durable.WithHandlesCache()(ctx)
	})
	t.Run("should panic if handles already exist", func(t *testing.T) {
		testutil.PanicsWithErrorIs(t, durable.ErrHandlesAlreadyExists, func() {
			durable.WithHandlesCache()(ctx)
		})
	})
	t.Run("should return the newly created handles", func(t *testing.T) {
		handles, ok := durable.GetHandles(ctx)
		assert.True(t, ok)
		assert.NotNil(t, handles)
	})
}
