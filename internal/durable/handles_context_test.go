package durable_test

import (
	"bytes"
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

type fakeHandle struct {
	value, committed []byte
}

func (h *fakeHandle) Cleanup() error           { return nil }
func (h *fakeHandle) Flush() ([]byte, bool)    { return h.value, !bytes.Equal(h.value, h.committed) }
func (h *fakeHandle) OnCommitted(value []byte) { h.committed = value }

func TestHandlesFlushAndOnCommitted(t *testing.T) {
	handles := durable.NewHandles()
	a := &fakeHandle{value: []byte("a")}
	b := &fakeHandle{value: []byte("b")}
	handles.LoadOrCompute("a", func() durable.Handle { return a })
	handles.LoadOrCompute("b", func() durable.Handle { return b })

	// a value is reported at every flush until it is committed
	flushed := handles.Flush()
	assert.Equal(t, map[string][]byte{"a": []byte("a"), "b": []byte("b")}, flushed)
	assert.Equal(t, flushed, handles.Flush())

	handles.OnCommitted(flushed)
	assert.Empty(t, handles.Flush())
	assert.Equal(t, []byte("a"), a.committed)
	assert.Equal(t, []byte("b"), b.committed)
}
