package durable_test

import (
	"testing"

	"github.com/futura-platform/futura/internal/durable"
	"github.com/futura-platform/futura/internal/utils/testutil"
	"github.com/stretchr/testify/assert"
)

func TestWithHandles(t *testing.T) {
	ctx := t.Context()

	// there are no handles before the cache is put on the context
	handles, ok := durable.GetHandles(ctx)
	assert.False(t, ok)
	assert.Nil(t, handles)

	cache := durable.NewHandles()
	ctx = durable.WithHandles(ctx, cache)
	handles, ok = durable.GetHandles(ctx)
	assert.True(t, ok)
	assert.Same(t, cache, handles)

	// a context carries one cache
	testutil.PanicsWithErrorIs(t, durable.ErrHandlesAlreadyExists, func() {
		durable.WithHandles(ctx, durable.NewHandles())
	})
}
