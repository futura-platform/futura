package execution

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"iter"
	"sync/atomic"
	"testing"

	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/replay"
	"github.com/futura-platform/futura/internal/flow/replay/sequence"
	"github.com/futura-platform/futura/internal/goroutinebind"
	"github.com/futura-platform/futura/internal/utils/containertest"
	"github.com/futura-platform/futura/internal/utils/testutil"
	"github.com/futura-platform/futura/moment"
	"github.com/petermattis/goid"
	"github.com/stretchr/testify/assert"
)

// running marks exec as running for the duration of the test, then returns it.
// Use this in tests that exercise FromContext/MustFromContext outside of the
// production Loop entry point (which would otherwise panic with
// ErrFlowExecutionNotRunning).
func running(t *testing.T, exec *FlowExecution) *FlowExecution {
	t.Helper()
	stop, ok := exec.TryStartRun()
	if !ok {
		t.Fatalf("flow execution is already running")
	}
	t.Cleanup(stop)
	return exec
}

func TestFlowExecutionRunningLifecycle(t *testing.T) {
	t.Run("Running reflects TryStartRun and stop", func(t *testing.T) {
		exec := NewFlowExecutionWithContainer(containertest.NewInMemory())
		assert.False(t, exec.Running(), "fresh execution should not be running")

		stop, ok := exec.TryStartRun()
		assert.True(t, ok)
		assert.True(t, exec.Running())

		stop()
		assert.False(t, exec.Running())
	})

	t.Run("TryStartRun returns false while a run is in flight", func(t *testing.T) {
		exec := NewFlowExecutionWithContainer(containertest.NewInMemory())
		stop, ok := exec.TryStartRun()
		assert.True(t, ok)
		t.Cleanup(stop)

		stop2, ok2 := exec.TryStartRun()
		assert.False(t, ok2)
		assert.Nil(t, stop2)
	})

	t.Run("a stopped execution can be started again", func(t *testing.T) {
		exec := NewFlowExecutionWithContainer(containertest.NewInMemory())
		stop, ok := exec.TryStartRun()
		assert.True(t, ok)
		stop()

		stop2, ok2 := exec.TryStartRun()
		assert.True(t, ok2)
		t.Cleanup(stop2)
	})

	t.Run("FromContext panics when the execution has not started", func(t *testing.T) {
		exec := NewFlowExecutionWithContainer(containertest.NewInMemory())
		ctx := WithFlow(t.Context(), exec)
		assert.PanicsWithError(t,
			ftrerrors.InconsistentStateError(ErrFlowExecutionNotRunning).Error(),
			func() { _, _ = FromContext(ctx) },
		)
	})

	t.Run("FromContext panics after stop fires", func(t *testing.T) {
		exec := NewFlowExecutionWithContainer(containertest.NewInMemory())
		ctx := WithFlow(t.Context(), exec)
		stop, ok := exec.TryStartRun()
		assert.True(t, ok)

		_, ok = FromContext(ctx)
		assert.True(t, ok, "FromContext should succeed while running")

		stop()
		assert.PanicsWithError(t,
			ftrerrors.InconsistentStateError(ErrFlowExecutionNotRunning).Error(),
			func() { _, _ = FromContext(ctx) },
		)
	})

	t.Run("UnsafeFromContext skips the running check", func(t *testing.T) {
		exec := NewFlowExecutionWithContainer(containertest.NewInMemory())
		ctx := WithFlow(t.Context(), exec)
		// Even though we never started a run, this must not panic.
		assert.Same(t, exec, UnsafeFromContext(ctx))
	})
}

func TestWithFlow(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		ctx := t.Context()
		fOriginal := running(t, NewFlowExecutionWithContainer(containertest.NewInMemory()))
		ctx = WithFlow(ctx, fOriginal)
		f, ok := FromContext(ctx)
		assert.True(t, ok)
		assert.NotNil(t, f)
		assert.Equal(t, fOriginal, f)

		ctx2 := t.Context()
		f2, ok := FromContext(ctx2)
		assert.False(t, ok)
		assert.Nil(t, f2)
	})
	t.Run("nil flow execution case", func(t *testing.T) {
		ctx := t.Context()
		assert.Panics(t, func() {
			WithFlow(ctx, nil)
		})
	})
}

func TestGetFlowContext_WrongGoroutine(t *testing.T) {
	ctx := t.Context()
	ctx = WithFlow(ctx, running(t, NewFlowExecutionWithContainer(containertest.NewInMemory())))
	assert.NotPanics(t, func() { FromContext(ctx) })
	boundGoroutineID := goid.Get()
	t.Run("panics", func(t *testing.T) {
		expectedError := ftrerrors.InconsistentStateError(goroutinebind.ErrWrongGoroutine{
			BoundGoroutineID:    boundGoroutineID,
			ObservedGoroutineID: goid.Get(),
		})
		assert.PanicsWithError(t, expectedError.Error(), func() {
			FromContext(ctx)
		})
	})
}

func TestExpectedIdentity(t *testing.T) {
	t.Run("has expected call", func(t *testing.T) {
		ctx := t.Context()
		c := executiontype.NewInMemoryContainer()
		ctx = sequence.With(WithFlow(ctx, running(t, NewFlowExecutionWithContainer(containertest.NewStrict(c)))), DefaultReplayFlags)
		f := MustFromContext(ctx)

		c.AppendCallOrder(moment.Identity{})

		identity, ok := f.ExpectedIdentity(ctx)
		assert.True(t, ok)
		assert.Equal(t, c.CallOrderAt(0), identity)
	})
	t.Run("no expected call", func(t *testing.T) {
		ctx := t.Context()
		c := executiontype.NewInMemoryContainer()
		ctx = sequence.With(WithFlow(ctx, running(t, NewFlowExecutionWithContainer(containertest.NewStrict(c)))), DefaultReplayFlags)
		f := MustFromContext(ctx)

		_, ok := f.ExpectedIdentity(ctx)
		assert.False(t, ok)
	})
	t.Run("sequence index out of bounds", func(t *testing.T) {
		ctx := t.Context()
		c := executiontype.NewInMemoryContainer()
		ctx = sequence.With(WithFlow(ctx, running(t, NewFlowExecutionWithContainer(containertest.NewStrict(c)))), DefaultReplayFlags)
		f := MustFromContext(ctx)

		c.AppendCallOrder(moment.Identity{})
		c.AppendCallOrder(moment.Identity{})
		ctx = sequence.With(ctx, DefaultReplayFlags)
		for range 10 {
			sequence.Advance(ctx)
		}

		assert.PanicsWithError(t, ftrerrors.InconsistentStateError(SequenceIndexOutOfBoundsError{
			sequenceIndex:  10,
			sequenceLength: 2,
		}).Error(), func() {
			f.ExpectedIdentity(ctx)
		})
	})
}

func TestGetMoment(t *testing.T) {
	ctx := t.Context()
	c := executiontype.NewInMemoryContainer()
	ctx = sequence.With(WithFlow(ctx, running(t, NewFlowExecutionWithContainer(containertest.NewStrict(c)))), DefaultReplayFlags)
	f := MustFromContext(ctx)
	identity := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}}, moment.Callsite{})
	m := moment.NewMoment(1)
	c.SetMoment(identity, *m)

	r, ok := f.GetMoment(ctx, identity)
	assert.True(t, ok)
	assert.Equal(t, r, *m)
	_, ok = f.GetMoment(ctx, moment.Identity{})
	assert.False(t, ok)
}

func TestWriteBehind_RestartsTheCurrentReplay(t *testing.T) {
	t.Run("cancels the current replay with the restart cause", func(t *testing.T) {
		f := running(t, NewFlowExecutionWithContainer(containertest.NewInMemory()))
		replayCtx, _ := f.StartNewReplay(WithFlow(t.Context(), f))

		f.WriteBehind(t.Context(), "key", nil)

		cause := context.Cause(replayCtx)
		assert.ErrorIs(t, cause, ErrRestartReplay)
		assert.ErrorIs(t, cause, ErrWrittenBehind)
	})
	t.Run("cancels the replay that is current now, not one that was current when the caller started", func(t *testing.T) {
		f := running(t, NewFlowExecutionWithContainer(containertest.NewInMemory()))
		first, _ := f.StartNewReplay(WithFlow(t.Context(), f))
		replay.Cancel(first, nil)
		second, _ := f.StartNewReplay(WithFlow(t.Context(), f))

		f.WriteBehind(t.Context(), "key", nil)
		assert.ErrorIs(t, context.Cause(second), ErrRestartReplay)
		assert.NotErrorIs(t, context.Cause(first), ErrRestartReplay)
	})
	t.Run("does not need a replay to restart", func(t *testing.T) {
		f := running(t, NewFlowExecutionWithContainer(containertest.NewInMemory()))
		assert.NotPanics(t, func() { f.WriteBehind(t.Context(), "key", nil) })
	})
	t.Run("does not restart a replay that has already ended", func(t *testing.T) {
		f := running(t, NewFlowExecutionWithContainer(containertest.NewInMemory()))
		replayCtx, _ := f.StartNewReplay(WithFlow(t.Context(), f))
		replay.Cancel(replayCtx, nil)

		f.WriteBehind(t.Context(), "key", nil)
		// cancellation is idempotent: the first cause stands
		assert.NotErrorIs(t, context.Cause(replayCtx), ErrRestartReplay)
	})
	t.Run("restarts before the value can be read, so nothing on the old replay can act on it", func(t *testing.T) {
		f := running(t, NewFlowExecutionWithContainer(containertest.NewInMemory()))
		replayCtx, _ := f.StartNewReplay(WithFlow(t.Context(), f))

		// observe the write from another goroutine the instant it lands
		seen := make(chan bool)
		go func() {
			for {
				if _, ok := f.ReadBehind(WithFlow(t.Context(), f), "key"); ok {
					seen <- replayCtx.Err() != nil
					return
				}
			}
		}()
		f.WriteBehind(t.Context(), "key", []byte{1})
		assert.True(t, <-seen, "the replay must already be cancelled when the value becomes readable")
	})
}

func TestStartNewReplay_CodeVersion(t *testing.T) {
	t.Run("a changed version bumps the dirty epoch once", func(t *testing.T) {
		f := running(t, NewFlowExecutionWithContainer(containertest.NewInMemory()))
		start := func(version string) uint64 {
			replayCtx, epoch := f.StartNewReplay(WithCodeVersion(WithFlow(t.Context(), f), version))
			replay.Cancel(replayCtx, nil)
			return epoch
		}
		assert.Equal(t, uint64(1), start("1"))
		assert.Equal(t, uint64(1), start("1"))
		assert.Equal(t, uint64(2), start("2"))
		assert.Equal(t, uint64(2), start("2"))
	})
	t.Run("no version leaves the dirty epoch alone", func(t *testing.T) {
		f := running(t, NewFlowExecutionWithContainer(containertest.NewInMemory()))
		replayCtx, epoch := f.StartNewReplay(WithFlow(t.Context(), f))
		replay.Cancel(replayCtx, nil)
		assert.Equal(t, uint64(0), epoch)
	})
}

func TestStartNewReplay(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		ctx := t.Context()
		ctx = sequence.With(WithFlow(ctx, running(t, NewFlowExecutionWithContainer(containertest.NewInMemory()))), DefaultReplayFlags)
		f := MustFromContext(ctx)
		replayCtx, _ := f.StartNewReplay(ctx)
		assert.True(t, replay.Has(replayCtx))
	})
	t.Run("a boundary that fails to commit derives no replay", func(t *testing.T) {
		f := running(t, NewFlowExecutionWithContainer(rejectingContainer{executiontype.NewInMemoryContainer()}))
		testutil.PanicsWithErrorIs(t, ErrTransactionFailed, func() { f.StartNewReplay(WithFlow(t.Context(), f)) })
		assert.Nil(t, f.cancelCurrentReplay)
	})
}

func TestLoadDurable_RetriesThroughTheContainer(t *testing.T) {
	t.Run("a read error is returned to the container so it can retry", func(t *testing.T) {
		c := &flakyDurableContainer{InMemoryContainer: executiontype.NewInMemoryContainer(), failures: 2}
		f := running(t, NewFlowExecutionWithContainer(c))
		ctx := WithFlow(t.Context(), f)
		f.WriteBehind(ctx, "key", []byte{1})
		f.StartNewReplay(ctx)
		// the boundary above consumed the first two failures itself; arm the read under test
		c.failures = 2
		c.attempts.Store(0)

		value, ok := f.LoadDurable(ctx, "key")
		assert.True(t, ok)
		assert.Equal(t, []byte{1}, value)
		assert.Equal(t, int32(3), c.attempts.Load(), "the container retried the read twice before it succeeded")

		c.failures = 2
		c.attempts.Store(0)
		value, ok = f.ReadBehind(ctx, "key")
		assert.True(t, ok)
		assert.Equal(t, []byte{1}, value)
		assert.Equal(t, int32(3), c.attempts.Load())
	})
	t.Run("a container that gives up surfaces the transaction failure", func(t *testing.T) {
		c := &flakyDurableContainer{InMemoryContainer: executiontype.NewInMemoryContainer(), failures: 100}
		f := running(t, NewFlowExecutionWithContainer(c))
		ctx := WithFlow(t.Context(), f)
		testutil.PanicsWithErrorIs(t, ErrTransactionFailed, func() { f.LoadDurable(ctx, "key") })
	})
}

// flakyDurableContainer is a backend whose durable reads and writes fail transiently: a
// transaction is retried only if the closure returns the error. A panic inside the closure is
// not caught, so a helper that panics on a transient error fails the test instead of being
// retried.
type flakyDurableContainer struct {
	*executiontype.InMemoryContainer
	failures      int
	writeFailures int
	attempts      atomic.Int32
}

var errFlakyRead = errors.New("request for future version")

func (c *flakyDurableContainer) retry(fn func() error) error {
	for range 3 {
		c.attempts.Add(1)
		err := fn()
		if err == nil {
			return nil
		}
		if !errors.Is(err, errFlakyRead) {
			return err
		}
	}
	return errFlakyRead
}

func (c *flakyDurableContainer) ReadTransact(ctx context.Context, fn func(context.Context, executiontype.ReadOnlyContainer) error) error {
	return c.retry(func() error { return fn(ctx, flakyTx{c}) })
}

func (c *flakyDurableContainer) Transact(ctx context.Context, fn func(context.Context, executiontype.Container) error) error {
	return c.retry(func() error { return fn(ctx, flakyTx{c}) })
}

type flakyTx struct{ c *flakyDurableContainer }

func (r flakyTx) CallOrderLength() int                               { return r.c.CallOrderLength() }
func (r flakyTx) CallOrderAt(i int) moment.Identity                  { return r.c.CallOrderAt(i) }
func (r flakyTx) HasMoment(id moment.Identity) bool                  { return r.c.HasMoment(id) }
func (r flakyTx) GetMoment(id moment.Identity) (moment.Moment, bool) { return r.c.GetMoment(id) }
func (r flakyTx) KnownMoments() iter.Seq[moment.Identity]            { return r.c.KnownMoments() }
func (r flakyTx) SetCallOrderAt(i int, id moment.Identity)           { r.c.SetCallOrderAt(i, id) }
func (r flakyTx) AppendCallOrder(id moment.Identity)                 { r.c.AppendCallOrder(id) }
func (r flakyTx) TruncateCallOrderAt(i int)                          { r.c.TruncateCallOrderAt(i) }
func (r flakyTx) SetMoment(id moment.Identity, m moment.Moment)      { r.c.SetMoment(id, m) }
func (r flakyTx) DeleteMoment(id moment.Identity)                    { r.c.DeleteMoment(id) }
func (r flakyTx) LoadDurable(key string) ([]byte, bool, error) {
	if r.c.failures > 0 {
		r.c.failures--
		return nil, false, errFlakyRead
	}
	return r.c.LoadDurable(key)
}
func (r flakyTx) StoreDurable(key string, value []byte) error {
	if r.c.writeFailures > 0 {
		r.c.writeFailures--
		return errFlakyRead
	}
	return r.c.StoreDurable(key, value)
}

func TestBoundaries_RetryThroughTheContainer(t *testing.T) {
	t.Run("a durable read inside a boundary is returned to the container so it can retry", func(t *testing.T) {
		// StartNewReplay reads the epochs (getEpoch) and the code version before it writes
		c := &flakyDurableContainer{InMemoryContainer: executiontype.NewInMemoryContainer(), failures: 2}
		f := running(t, NewFlowExecutionWithContainer(c))
		ctx := WithCodeVersion(WithFlow(t.Context(), f), "v1")
		replayCtx, epoch := f.StartNewReplay(ctx)
		replay.Cancel(replayCtx, nil)
		assert.Equal(t, uint64(1), epoch, "the version bump landed once the read succeeded")
		assert.Equal(t, int32(3), c.attempts.Load(), "the container retried the boundary twice before it succeeded")
	})
	t.Run("a durable write inside a boundary is returned to the container so it can retry", func(t *testing.T) {
		c := &flakyDurableContainer{InMemoryContainer: executiontype.NewInMemoryContainer(), writeFailures: 2}
		f := running(t, NewFlowExecutionWithContainer(c))
		ctx := WithFlow(t.Context(), f)
		f.WriteBehind(ctx, "key", []byte{1})
		replayCtx, _ := f.StartNewReplay(ctx)
		replay.Cancel(replayCtx, nil)
		assert.Equal(t, int32(3), c.attempts.Load())
		value, ok := f.LoadDurable(ctx, "key")
		assert.True(t, ok)
		assert.Equal(t, []byte{1}, value, "the flush committed on the retry that succeeded")
	})
	t.Run("a settle whose read keeps failing surfaces the transaction failure", func(t *testing.T) {
		c := &flakyDurableContainer{InMemoryContainer: executiontype.NewInMemoryContainer(), failures: 100}
		f := running(t, NewFlowExecutionWithContainer(c))
		ctx := WithFlow(t.Context(), f)
		testutil.PanicsWithErrorIs(t, ErrTransactionFailed, func() { f.SettleSequence(ctx, -1, 0) })
	})
}

// rejectingContainer is a container that rejects every write transaction.
type rejectingContainer struct {
	*executiontype.InMemoryContainer
}

func (rejectingContainer) Transact(context.Context, func(context.Context, executiontype.Container) error) error {
	return errors.New("commit failed")
}

func TestRecordCurrentMoment(t *testing.T) {
	t.Run("fresh moment case", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		ctx := sequence.With(WithFlow(t.Context(), running(t, NewFlowExecutionWithContainer(containertest.NewStrict(c)))), DefaultReplayFlags)
		f := MustFromContext(ctx)

		recordKey := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}}, moment.Callsite{})
		recordMoment := moment.NewMoment(1)

		f.RecordCurrentMoment(ctx, recordKey, *recordMoment)
		assert.Equal(t, c.CallOrderAt(0), recordKey)
		assert.Equal(t, sequence.GetIndex(ctx), 0)
		m, _ := c.GetMoment(recordKey)
		assert.Equal(t, m, *recordMoment)
	})
	t.Run("existing moment case", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		ctx := sequence.With(WithFlow(t.Context(), running(t, NewFlowExecutionWithContainer(containertest.NewStrict(c)))), DefaultReplayFlags)
		f := MustFromContext(ctx)

		recordKey := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}}, moment.Callsite{})
		recordMoment := moment.NewMoment(1)

		c.SetMoment(recordKey, *recordMoment)
		c.AppendCallOrder(recordKey)
		assert.False(t, sequence.IsSeen(ctx, recordKey))

		f.RecordCurrentMoment(ctx, recordKey, *recordMoment)
		assert.True(t, sequence.IsSeen(ctx, recordKey))
		assert.Equal(t, c.CallOrderAt(0), recordKey)
		assert.Equal(t, sequence.GetIndex(ctx), 0)
		m, _ := c.GetMoment(recordKey)
		assert.Equal(t, m, *recordMoment)
	})
	t.Run("survives the container retrying the transaction", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		strict := containertest.NewStrict(c)
		ctx := sequence.With(WithFlow(t.Context(), running(t, NewFlowExecutionWithContainer(strict))), DefaultReplayFlags)
		f := MustFromContext(ctx)

		recordKey := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}}, moment.Callsite{})
		recordMoment := moment.NewMoment(1)

		f.RecordCurrentMoment(ctx, recordKey, *recordMoment)

		assert.Equal(t, int64(containertest.Attempts), strict.Calls.Load(), "the closure should have run once per attempt")
		assert.Equal(t, 1, c.CallOrderLength(), "recorded exactly once")
		assert.True(t, c.HasMoment(recordKey))
		assert.True(t, sequence.IsSeen(ctx, recordKey))
	})
	t.Run("with existing cached state case", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		ctx := WithFlow(t.Context(), running(t, NewFlowExecutionWithContainer(containertest.NewStrict(c))))
		f := MustFromContext(ctx)

		recordKey := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}}, moment.Callsite{})
		recordMoment := moment.NewMoment(1)
		c.SetMoment(recordKey, *recordMoment)

		t.Run("append to call order", func(t *testing.T) {
			ctx = sequence.With(ctx, DefaultReplayFlags)

			assert.False(t, sequence.IsSeen(ctx, recordKey))

			f.RecordCurrentMoment(ctx, recordKey, *recordMoment)
			assert.True(t, sequence.IsSeen(ctx, recordKey))
			assert.Equal(t, c.CallOrderAt(0), recordKey)
			assert.Equal(t, sequence.GetIndex(ctx), 0)
			m, _ := c.GetMoment(recordKey)
			assert.Equal(t, m, *recordMoment)

			assert.True(t, sequence.IsSeen(ctx, recordKey))
		})

		t.Run("set call order at existing index", func(t *testing.T) {
			assert.Equal(t, c.CallOrderLength(), 1)
			ctx = sequence.With(ctx, DefaultReplayFlags)

			f.RecordCurrentMoment(ctx, recordKey, *recordMoment)
			assert.True(t, sequence.IsSeen(ctx, recordKey))
			assert.Equal(t, c.CallOrderAt(0), recordKey)
			assert.Equal(t, sequence.GetIndex(ctx), 0)
			m, _ := c.GetMoment(recordKey)
			assert.Equal(t, m, *recordMoment)

			assert.True(t, sequence.IsSeen(ctx, recordKey))
		})
	})
	t.Run("sequence index out of bounds", func(t *testing.T) {
		ctx := t.Context()
		c := executiontype.NewInMemoryContainer()
		ctx = sequence.With(WithFlow(ctx, running(t, NewFlowExecutionWithContainer(containertest.NewStrict(c)))), DefaultReplayFlags)
		f := MustFromContext(ctx)

		ctx = sequence.With(ctx, DefaultReplayFlags)
		for range 10 {
			sequence.Advance(ctx)
		}
		c.AppendCallOrder(moment.Identity{})
		c.AppendCallOrder(moment.Identity{})

		assert.PanicsWithError(t, ftrerrors.InconsistentStateError(SequenceIndexOutOfBoundsError{
			sequenceIndex:  10,
			sequenceLength: 2,
		}).Error(), func() {
			f.RecordCurrentMoment(ctx, moment.Identity{}, moment.Moment{})
		})
	})
}

func TestWriteBehind(t *testing.T) {
	getEpoch := func(t *testing.T, c *executiontype.InMemoryContainer) uint64 {
		t.Helper()
		encoded, ok, err := c.LoadDurable(dirtyEpochKey)
		assert.NoError(t, err)
		if !ok {
			return 0
		}
		var epoch uint64
		_, err = binary.Decode(encoded, binary.LittleEndian, &epoch)
		assert.NoError(t, err)
		return epoch
	}
	stored := func(t *testing.T, c *executiontype.InMemoryContainer, key string) ([]byte, bool) {
		t.Helper()
		value, ok, err := c.LoadDurable(GenericDurableKey(key))
		assert.NoError(t, err)
		return value, ok
	}
	startReplay := func(t *testing.T, f *FlowExecution) (dirtyEpoch uint64) {
		t.Helper()
		_, dirtyEpoch = f.StartNewReplay(WithFlow(t.Context(), f))
		return dirtyEpoch
	}

	t.Run("the value is readable immediately, but nothing is written until the next replay starts", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		f := running(t, NewFlowExecutionWithContainer(containertest.NewStrict(c)))
		ctx := WithFlow(t.Context(), f)

		f.WriteBehind(t.Context(), "key", []byte{1})
		value, ok := f.ReadBehind(ctx, "key")
		assert.True(t, ok)
		assert.Equal(t, []byte{1}, value)
		_, ok = f.LoadDurable(ctx, "key")
		assert.False(t, ok, "LoadDurable reads the container only")
		_, ok = stored(t, c, "key")
		assert.False(t, ok)
		assert.Equal(t, uint64(0), getEpoch(t, c))

		dirtyEpoch := startReplay(t, f)
		value, ok = stored(t, c, "key")
		assert.True(t, ok)
		assert.Equal(t, []byte{1}, value)
		assert.Equal(t, uint64(1), getEpoch(t, c))
		// the replay that flushed the write was started against the bumped epoch
		assert.Equal(t, uint64(1), dirtyEpoch)
	})
	t.Run("every dirty value is flushed in one transaction with a single epoch bump", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		f := running(t, NewFlowExecutionWithContainer(containertest.NewStrict(c)))

		f.WriteBehind(t.Context(), "first", []byte{1})
		f.WriteBehind(t.Context(), "second", []byte{2})
		startReplay(t, f)

		for key, expected := range map[string][]byte{"first": {1}, "second": {2}} {
			value, ok := stored(t, c, key)
			assert.True(t, ok, key)
			assert.Equal(t, expected, value, key)
		}
		assert.Equal(t, uint64(1), getEpoch(t, c))
	})
	t.Run("the latest write to a key wins", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		f := running(t, NewFlowExecutionWithContainer(containertest.NewStrict(c)))
		ctx := WithFlow(t.Context(), f)

		f.WriteBehind(t.Context(), "key", []byte{1})
		f.WriteBehind(t.Context(), "key", []byte{2})
		value, _ := f.ReadBehind(ctx, "key")
		assert.Equal(t, []byte{2}, value)

		startReplay(t, f)
		value, _ = stored(t, c, "key")
		assert.Equal(t, []byte{2}, value)
	})
	t.Run("a flushed value is read from the container afterwards", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		f := running(t, NewFlowExecutionWithContainer(containertest.NewStrict(c)))
		ctx := WithFlow(t.Context(), f)

		f.WriteBehind(t.Context(), "key", []byte{1})
		startReplay(t, f)
		assert.NoError(t, c.StoreDurable(GenericDurableKey("key"), []byte{9}))

		value, _ := f.ReadBehind(ctx, "key")
		assert.Equal(t, []byte{9}, value, "nothing is dirty after the flush, so the container is authoritative")
	})
	t.Run("a replay started with nothing dirty does not bump the epoch", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		f := running(t, NewFlowExecutionWithContainer(containertest.NewStrict(c)))

		f.WriteBehind(t.Context(), "key", nil)
		startReplay(t, f)
		assert.Equal(t, uint64(1), getEpoch(t, c))

		startReplay(t, f)
		startReplay(t, f)
		assert.Equal(t, uint64(1), getEpoch(t, c))
	})
	t.Run("flushing survives the container retrying the transaction", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		strict := containertest.NewStrict(c)
		f := running(t, NewFlowExecutionWithContainer(strict))

		f.WriteBehind(t.Context(), "key", []byte{1})
		dirtyEpoch := startReplay(t, f)

		assert.Equal(t, int64(containertest.Attempts), strict.Calls.Load(), "the closure should have run once per attempt")
		assert.Equal(t, uint64(1), getEpoch(t, c), "but the epoch is bumped exactly once")
		assert.Equal(t, uint64(1), dirtyEpoch)
		_, ok := stored(t, c, "key")
		assert.True(t, ok)
	})
	t.Run("the replay's flags come from the attempt that committed, not an earlier one", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		strict := containertest.NewStrict(c)
		strict.StaleView = func(tx executiontype.Container) {
			// the discarded attempt sees an epoch that would relax the replay
			encoded, err := binary.Append(nil, binary.LittleEndian, uint64(5))
			assert.NoError(t, err)
			assert.NoError(t, tx.StoreDurable(dirtyEpochKey, encoded))
		}
		f := running(t, NewFlowExecutionWithContainer(strict))

		replayCtx, _ := f.StartNewReplay(WithFlow(t.Context(), f))
		assert.True(t, sequence.GetFlags(replayCtx).PanicOnMomentOrderChange)
	})
	t.Run("a write between runs is flushed by the next run's first replay", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		f := NewFlowExecutionWithContainer(containertest.NewStrict(c))

		stop, ok := f.TryStartRun()
		assert.True(t, ok)
		stop()

		f.WriteBehind(t.Context(), "key", []byte{1})
		assert.Equal(t, uint64(0), getEpoch(t, c))

		stop, ok = f.TryStartRun()
		assert.True(t, ok)
		defer stop()
		startReplay(t, f)
		assert.Equal(t, uint64(1), getEpoch(t, c))
		_, ok = stored(t, c, "key")
		assert.True(t, ok)
	})
}

func TestSettleSequence(t *testing.T) {
	t.Run("survives the container retrying the transaction", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		for range 3 {
			c.AppendCallOrder(moment.Identity{})
		}
		strict := containertest.NewStrict(c)
		f := NewFlowExecutionWithContainer(strict)

		f.SettleSequence(t.Context(), 1, 4)

		assert.Equal(t, int64(containertest.Attempts), strict.Calls.Load(), "the closure should have run once per attempt")
		assert.Equal(t, 2, c.CallOrderLength(), "truncated to the recorded index exactly once")
		encoded, ok, err := c.LoadDurable(evaluatedEpochKey)
		assert.NoError(t, err)
		assert.True(t, ok)
		var epoch uint64
		_, err = binary.Decode(encoded, binary.LittleEndian, &epoch)
		assert.NoError(t, err)
		assert.Equal(t, uint64(4), epoch)
	})
	t.Run("panics if the evaluated epoch would move backwards", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		f := NewFlowExecutionWithContainer(containertest.NewStrict(c))
		ctx := t.Context()

		f.SettleSequence(ctx, -1, 2)
		testutil.PanicsWithErrorIs(t, ErrEpochRegression, func() {
			f.SettleSequence(ctx, -1, 1)
		})
	})
	t.Run("does not read what it wrote in the same transaction", func(t *testing.T) {
		// a container may apply a transaction's writes when it commits, so a read inside it sees the state before
		c := executiontype.NewInMemoryContainer()
		for range 3 {
			c.AppendCallOrder(moment.Identity{})
		}
		f := NewFlowExecutionWithContainer(containertest.NewStrict(&applyOnCommit{InMemoryContainer: c}))
		f.SettleSequence(t.Context(), 1, 1)
		assert.Equal(t, 2, c.CallOrderLength())
	})
	t.Run("panics if the call order does not end where the replay stopped", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		f := NewFlowExecutionWithContainer(containertest.NewStrict(c))

		// the record is empty, but the replay claims to have recorded an entry
		testutil.PanicsWithErrorIs(t, ErrSettledSequenceMismatch, func() {
			f.SettleSequence(t.Context(), 0, 0)
		})
	})
}

func TestDurable(t *testing.T) {
	c := executiontype.NewInMemoryContainer()
	f := NewFlowExecutionWithContainer(containertest.NewStrict(c))
	ctx := sequence.With(WithFlow(t.Context(), f), DefaultReplayFlags)
	t.Run("returns !ok if the value is not found", func(t *testing.T) {
		_, ok := f.LoadDurable(ctx, "notFound")
		assert.False(t, ok)
	})
	t.Run("loads a stored value", func(t *testing.T) {
		value := byte(100)
		assert.NoError(t, c.StoreDurable(GenericDurableKey("test"), []byte{value}))
		loaded, ok := f.LoadDurable(ctx, "test")
		assert.True(t, ok)
		assert.Equal(t, loaded, []byte{value})
	})
}

func TestMustFromContext(t *testing.T) {
	ctx := t.Context()
	ctx = sequence.With(WithFlow(ctx, running(t, NewFlowExecutionWithContainer(containertest.NewInMemory()))), DefaultReplayFlags)
	f := MustFromContext(ctx)
	assert.NotNil(t, f)

	ctx2 := t.Context()
	assert.PanicsWithError(t, ErrFlowContextNotFound.Error(), func() {
		MustFromContext(ctx2)
	})
}

func TestNewFlowExecutionFromState(t *testing.T) {
	c := executiontype.NewInMemoryContainer()
	f := NewFlowExecutionWithContainer(c)
	assert.NotNil(t, f)
	assert.Equal(t, c, f.c)
}

// recyclingContainer hands out bytes that it overwrites once the read transaction ends, like a page-backed store.
type recyclingContainer struct {
	*executiontype.InMemoryContainer
}

type recyclingTx struct {
	executiontype.ReadOnlyContainer
	handedOut [][]byte
}

func (tx *recyclingTx) LoadDurable(key string) ([]byte, bool, error) {
	value, ok, err := tx.ReadOnlyContainer.LoadDurable(key)
	page := bytes.Clone(value)
	tx.handedOut = append(tx.handedOut, page)
	return page, ok, err
}

func (c *recyclingContainer) ReadTransact(ctx context.Context, fn func(ctx context.Context, tx executiontype.ReadOnlyContainer) error) error {
	return c.InMemoryContainer.ReadTransact(ctx, func(ctx context.Context, tx executiontype.ReadOnlyContainer) error {
		recycling := &recyclingTx{ReadOnlyContainer: tx}
		defer func() {
			for _, page := range recycling.handedOut {
				clear(page)
			}
		}()
		return fn(ctx, recycling)
	})
}

func TestDurableReadsOutliveTheirTransaction(t *testing.T) {
	c := &recyclingContainer{InMemoryContainer: executiontype.NewInMemoryContainer()}
	assert.NoError(t, c.StoreDurable(GenericDurableKey("key"), []byte("value")))
	f := running(t, NewFlowExecutionWithContainer(containertest.NewStrict(c)))
	ctx := WithFlow(t.Context(), f)

	value, ok := f.LoadDurable(ctx, "key")
	assert.True(t, ok)
	assert.Equal(t, []byte("value"), value)

	value, ok = f.ReadBehind(ctx, "key")
	assert.True(t, ok)
	assert.Equal(t, []byte("value"), value)
}

// applyOnCommit queues a transaction's call-order writes and applies them once it returns, so reads
// inside the transaction see the state before it.
type applyOnCommit struct {
	*executiontype.InMemoryContainer
}

type queuedTx struct {
	executiontype.Container
	queued []func()
}

func (tx *queuedTx) TruncateCallOrderAt(index int) {
	tx.queued = append(tx.queued, func() { tx.Container.TruncateCallOrderAt(index) })
}

func (c *applyOnCommit) Transact(ctx context.Context, fn func(ctx context.Context, tx executiontype.Container) error) error {
	return c.InMemoryContainer.Transact(ctx, func(ctx context.Context, tx executiontype.Container) error {
		queued := &queuedTx{Container: tx}
		if err := fn(ctx, queued); err != nil {
			return err
		}
		for _, apply := range queued.queued {
			apply()
		}
		return nil
	})
}
