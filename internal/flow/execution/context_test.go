package execution

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/futura-platform/futura/ftype/executiontype"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
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
		exec := NewFlowExecution()
		assert.False(t, exec.Running(), "fresh execution should not be running")

		stop, ok := exec.TryStartRun()
		assert.True(t, ok)
		assert.True(t, exec.Running())

		stop()
		assert.False(t, exec.Running())
	})

	t.Run("TryStartRun returns false while a run is in flight", func(t *testing.T) {
		exec := NewFlowExecution()
		stop, ok := exec.TryStartRun()
		assert.True(t, ok)
		t.Cleanup(stop)

		stop2, ok2 := exec.TryStartRun()
		assert.False(t, ok2)
		assert.Nil(t, stop2)
	})

	t.Run("a stopped execution can be started again", func(t *testing.T) {
		exec := NewFlowExecution()
		stop, ok := exec.TryStartRun()
		assert.True(t, ok)
		stop()

		stop2, ok2 := exec.TryStartRun()
		assert.True(t, ok2)
		t.Cleanup(stop2)
	})

	t.Run("FromContext panics when the execution has not started", func(t *testing.T) {
		exec := NewFlowExecution()
		ctx := WithFlow(t.Context(), exec)
		assert.PanicsWithError(t,
			ftrerrors.InconsistentStateError(ErrFlowExecutionNotRunning).Error(),
			func() { _, _ = FromContext(ctx) },
		)
	})

	t.Run("FromContext panics after stop fires", func(t *testing.T) {
		exec := NewFlowExecution()
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
		exec := NewFlowExecution()
		ctx := WithFlow(t.Context(), exec)
		// Even though we never started a run, this must not panic.
		assert.Same(t, exec, UnsafeFromContext(ctx))
	})
}

func TestWithFlow(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		ctx := t.Context()
		fOriginal := running(t, NewFlowExecution())
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
	ctx = WithFlow(ctx, running(t, NewFlowExecution()))
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

func TestCancelCurrentReplay(t *testing.T) {
	f := running(t, NewFlowExecution())
	assert.PanicsWithError(t, ftrerrors.InconsistentStateError(ErrNilCancellationCause).Error(), func() {
		f.RestartCurrentReplay(nil)
	})
}

func TestExpectedIdentity(t *testing.T) {
	t.Run("has expected call", func(t *testing.T) {
		ctx := t.Context()
		c := executiontype.NewInMemoryContainer()
		ctx = sequence.With(WithFlow(ctx, running(t, NewFlowExecutionWithContainer(c))), DefaultReplayFlags)
		f := MustFromContext(ctx)

		c.AppendCallOrder(moment.Identity{})

		identity, ok := f.ExpectedIdentity(ctx)
		assert.True(t, ok)
		assert.Equal(t, c.CallOrderAt(0), identity)
	})
	t.Run("no expected call", func(t *testing.T) {
		ctx := t.Context()
		c := executiontype.NewInMemoryContainer()
		ctx = sequence.With(WithFlow(ctx, running(t, NewFlowExecutionWithContainer(c))), DefaultReplayFlags)
		f := MustFromContext(ctx)

		_, ok := f.ExpectedIdentity(ctx)
		assert.False(t, ok)
	})
	t.Run("sequence index out of bounds", func(t *testing.T) {
		ctx := t.Context()
		c := executiontype.NewInMemoryContainer()
		ctx = sequence.With(WithFlow(ctx, running(t, NewFlowExecutionWithContainer(c))), DefaultReplayFlags)
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

var placeholderCallable = moment.NewFn[struct{}, struct{}](func(ctx context.Context, args struct{}) (struct{}, error) {
	return struct{}{}, nil
})

func TestGetMoment(t *testing.T) {
	ctx := t.Context()
	c := executiontype.NewInMemoryContainer()
	ctx = sequence.With(WithFlow(ctx, running(t, NewFlowExecutionWithContainer(c))), DefaultReplayFlags)
	f := MustFromContext(ctx)
	identity := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}})
	m := moment.NewMoment(placeholderCallable, 1)
	c.SetMoment(identity, *m)

	r, ok := f.GetMoment(ctx, identity)
	assert.True(t, ok)
	assert.Equal(t, r, *m)
	_, ok = f.GetMoment(ctx, moment.Identity{})
	assert.False(t, ok)
}

func TestRestartCurrentReplay(t *testing.T) {
	t.Run("cancels the current replay with the restart cause", func(t *testing.T) {
		f := running(t, NewFlowExecution())
		replayCtx, _ := f.StartNewReplay(WithFlow(t.Context(), f))

		cancelCause := errors.New("placeholder")
		f.RestartCurrentReplay(cancelCause)

		cause := context.Cause(replayCtx)
		assert.ErrorIs(t, cause, ErrRestartReplay)
		assert.ErrorIs(t, cause, cancelCause)
	})
	t.Run("cancels the replay that is current now, not one that was current when the caller started", func(t *testing.T) {
		f := running(t, NewFlowExecution())
		first, _ := f.StartNewReplay(WithFlow(t.Context(), f))
		replay.Cancel(first, nil)
		second, _ := f.StartNewReplay(WithFlow(t.Context(), f))

		f.RestartCurrentReplay(errors.New("from a holder of the first replay"))
		assert.ErrorIs(t, context.Cause(second), ErrRestartReplay)
		assert.NotErrorIs(t, context.Cause(first), ErrRestartReplay)
	})
	t.Run("is a no-op before any replay has started", func(t *testing.T) {
		f := running(t, NewFlowExecution())
		assert.NotPanics(t, func() { f.RestartCurrentReplay(errors.New("nothing to restart")) })
	})
	t.Run("is a no-op after the current replay has ended", func(t *testing.T) {
		f := running(t, NewFlowExecution())
		replayCtx, _ := f.StartNewReplay(WithFlow(t.Context(), f))
		replay.Cancel(replayCtx, nil)

		f.RestartCurrentReplay(errors.New("after the replay ended"))
		// cancellation is idempotent: the first cause stands
		assert.NotErrorIs(t, context.Cause(replayCtx), ErrRestartReplay)
	})
	t.Run("panics without a cause", func(t *testing.T) {
		f := running(t, NewFlowExecution())
		assert.PanicsWithError(t, ftrerrors.InconsistentStateError(ErrNilCancellationCause).Error(), func() {
			f.RestartCurrentReplay(nil)
		})
	})
}

func TestStartNewReplay(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		ctx := t.Context()
		ctx = sequence.With(WithFlow(ctx, running(t, NewFlowExecution())), DefaultReplayFlags)
		f := MustFromContext(ctx)
		replayCtx, _ := f.StartNewReplay(ctx)
		assert.True(t, replay.Has(replayCtx))
	})
}

func TestRecordCurrentMoment(t *testing.T) {
	t.Run("fresh moment case", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		ctx := sequence.With(WithFlow(t.Context(), running(t, NewFlowExecutionWithContainer(c))), DefaultReplayFlags)
		f := MustFromContext(ctx)

		recordKey := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}})
		recordMoment := moment.NewMoment(placeholderCallable, 1)

		f.RecordCurrentMoment(ctx, recordKey, *recordMoment)
		assert.Equal(t, c.CallOrderAt(0), recordKey)
		assert.Equal(t, sequence.GetIndex(ctx), 0)
		m, _ := c.GetMoment(recordKey)
		assert.Equal(t, m, *recordMoment)
	})
	t.Run("existing moment case", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		ctx := sequence.With(WithFlow(t.Context(), running(t, NewFlowExecutionWithContainer(c))), DefaultReplayFlags)
		f := MustFromContext(ctx)

		recordKey := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}})
		recordMoment := moment.NewMoment(placeholderCallable, 1)

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
		retrying := containertest.NewRetrying(c, 3)
		ctx := sequence.With(WithFlow(t.Context(), running(t, NewFlowExecutionWithContainer(retrying))), DefaultReplayFlags)
		f := MustFromContext(ctx)

		recordKey := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}})
		recordMoment := moment.NewMoment(placeholderCallable, 1)

		f.RecordCurrentMoment(ctx, recordKey, *recordMoment)

		assert.Equal(t, 3, retrying.Calls, "the closure should have run once per attempt")
		assert.Equal(t, 1, c.CallOrderLength(), "recorded exactly once")
		assert.True(t, c.HasMoment(recordKey))
		assert.True(t, sequence.IsSeen(ctx, recordKey))
	})
	t.Run("with existing cached state case", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		ctx := WithFlow(t.Context(), running(t, NewFlowExecutionWithContainer(c)))
		f := MustFromContext(ctx)

		recordKey := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}})
		recordMoment := moment.NewMoment(placeholderCallable, 1)
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

			assert.PanicsWithError(t, ftrerrors.InconsistentStateError(UnexpectedDuplicateMomentError{
				identity: recordKey,
			}).Error(), func() {
				f.RecordCurrentMoment(ctx, recordKey, *recordMoment)
			})
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

			assert.PanicsWithError(t, ftrerrors.InconsistentStateError(UnexpectedDuplicateMomentError{
				identity: recordKey,
			}).Error(), func() {
				f.RecordCurrentMoment(ctx, recordKey, *recordMoment)
			})
		})
	})
	t.Run("sequence index out of bounds", func(t *testing.T) {
		ctx := t.Context()
		c := executiontype.NewInMemoryContainer()
		ctx = sequence.With(WithFlow(ctx, running(t, NewFlowExecutionWithContainer(c))), DefaultReplayFlags)
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

func TestInvalidateSequence(t *testing.T) {
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
	startReplay := func(t *testing.T, f *FlowExecution) (dirtyEpoch uint64) {
		t.Helper()
		_, dirtyEpoch = f.StartNewReplay(WithFlow(t.Context(), f))
		return dirtyEpoch
	}

	t.Run("the mutation is applied immediately, but nothing is written until the next replay starts", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		f := running(t, NewFlowExecutionWithContainer(c))

		applied, writes := false, 0
		f.InvalidateSequence(func() { applied = true }, func(executiontype.Container) { writes++ })
		assert.True(t, applied)
		assert.Equal(t, uint64(0), getEpoch(t, c))
		assert.Equal(t, 0, writes)

		dirtyEpoch := startReplay(t, f)
		assert.Equal(t, uint64(1), getEpoch(t, c))
		assert.Equal(t, 1, writes)
		// the replay that committed the invalidation was started against the bumped epoch
		assert.Equal(t, uint64(1), dirtyEpoch)
	})
	t.Run("every pending write is committed in one transaction with a single epoch bump", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		f := running(t, NewFlowExecutionWithContainer(c))

		var order []string
		f.InvalidateSequence(func() {}, func(tx executiontype.Container) {
			order = append(order, "first")
			assert.NoError(t, tx.StoreDurable("first", []byte{1}))
		})
		f.InvalidateSequence(func() {}, func(tx executiontype.Container) {
			order = append(order, "second")
			assert.NoError(t, tx.StoreDurable("second", []byte{2}))
		})
		startReplay(t, f)

		assert.Equal(t, []string{"first", "second"}, order)
		for _, key := range []string{"first", "second"} {
			_, ok, err := c.LoadDurable(key)
			assert.NoError(t, err)
			assert.True(t, ok, key)
		}
		assert.Equal(t, uint64(1), getEpoch(t, c))
	})
	t.Run("a replay started with nothing pending does not bump the epoch", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		f := running(t, NewFlowExecutionWithContainer(c))

		f.InvalidateSequence(func() {}, func(executiontype.Container) {})
		startReplay(t, f)
		assert.Equal(t, uint64(1), getEpoch(t, c))

		startReplay(t, f)
		startReplay(t, f)
		assert.Equal(t, uint64(1), getEpoch(t, c))
	})
	t.Run("committing survives the container retrying the transaction", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		retrying := containertest.NewRetrying(c, 3)
		f := running(t, NewFlowExecutionWithContainer(retrying))

		writes := 0
		f.InvalidateSequence(func() {}, func(tx executiontype.Container) {
			writes++
			assert.NoError(t, tx.StoreDurable("value", []byte{1}))
		})
		dirtyEpoch := startReplay(t, f)

		assert.Equal(t, 3, retrying.Calls, "the closure should have run once per attempt")
		assert.Equal(t, 3, writes, "the write runs on every attempt, against a fresh transaction")
		assert.Equal(t, uint64(1), getEpoch(t, c), "but the epoch is bumped exactly once")
		assert.Equal(t, uint64(1), dirtyEpoch)
		_, ok, err := c.LoadDurable("value")
		assert.NoError(t, err)
		assert.True(t, ok)
	})
	t.Run("the replay's flags come from the attempt that committed, not an earlier one", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		retrying := containertest.NewRetrying(c, 2)
		retrying.StaleView = func(tx executiontype.Container) {
			// the discarded attempt sees an epoch that would relax the replay
			encoded, err := binary.Append(nil, binary.LittleEndian, uint64(5))
			assert.NoError(t, err)
			assert.NoError(t, tx.StoreDurable(dirtyEpochKey, encoded))
		}
		f := running(t, NewFlowExecutionWithContainer(retrying))

		replayCtx, _ := f.StartNewReplay(WithFlow(t.Context(), f))
		assert.True(t, sequence.GetFlags(replayCtx).PanicOnMomentOrderChange)
	})
	t.Run("a replay cannot start between a mutation being applied and its invalidation being recorded", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		f := running(t, NewFlowExecutionWithContainer(c))

		applied := make(chan struct{})
		release := make(chan struct{})
		invalidated := make(chan struct{})
		go func() {
			defer close(invalidated)
			f.InvalidateSequence(func() {
				close(applied)
				<-release // hold the lock with the mutation applied but the invalidation not yet recorded
			}, func(executiontype.Container) {})
		}()
		<-applied

		started := make(chan replay.Flags)
		go func() {
			replayCtx, _ := f.StartNewReplay(WithFlow(t.Context(), f))
			started <- sequence.GetFlags(replayCtx)
		}()

		select {
		case <-started:
			t.Fatal("a replay started while a mutation was applied but not yet invalidated")
		case <-time.After(50 * time.Millisecond):
		}

		close(release)
		<-invalidated
		flags := <-started
		assert.False(t, flags.PanicOnMomentOrderChange, "the replay that started after the invalidation must be relaxed")
	})
	t.Run("an invalidation recorded between runs is committed by the next run's first replay", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		f := NewFlowExecutionWithContainer(c)

		stop, ok := f.TryStartRun()
		assert.True(t, ok)
		stop()

		f.InvalidateSequence(func() {}, func(executiontype.Container) {})
		assert.Equal(t, uint64(0), getEpoch(t, c))

		stop, ok = f.TryStartRun()
		assert.True(t, ok)
		defer stop()
		startReplay(t, f)
		assert.Equal(t, uint64(1), getEpoch(t, c))
	})
}

func TestSettleSequence(t *testing.T) {
	t.Run("survives the container retrying the transaction", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		for range 3 {
			c.AppendCallOrder(moment.Identity{})
		}
		retrying := containertest.NewRetrying(c, 3)
		f := NewFlowExecutionWithContainer(retrying)

		f.SettleSequence(t.Context(), 1, 4)

		assert.Equal(t, 3, retrying.Calls, "the closure should have run once per attempt")
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
		f := NewFlowExecutionWithContainer(c)
		ctx := t.Context()

		f.SettleSequence(ctx, -1, 2)
		testutil.PanicsWithErrorIs(t, ErrEpochRegression, func() {
			f.SettleSequence(ctx, -1, 1)
		})
	})
	t.Run("panics if the call order does not end where the replay stopped", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		f := NewFlowExecutionWithContainer(c)

		// the record is empty, but the replay claims to have recorded an entry
		testutil.PanicsWithErrorIs(t, ErrSettledSequenceMismatch, func() {
			f.SettleSequence(t.Context(), 0, 0)
		})
	})
}

func TestDurable(t *testing.T) {
	c := executiontype.NewInMemoryContainer()
	f := NewFlowExecutionWithContainer(c)
	ctx := sequence.With(WithFlow(t.Context(), f), DefaultReplayFlags)
	t.Run("returns !ok if the value is not found", func(t *testing.T) {
		_, ok := f.LoadDurable(ctx, "notFound")
		assert.False(t, ok)
	})
	t.Run("can store and load a value", func(t *testing.T) {
		value := byte(100)
		f.StoreDurable(ctx, "test", []byte{value})
		loaded, ok := f.LoadDurable(ctx, "test")
		assert.True(t, ok)
		assert.Equal(t, loaded, []byte{value})
	})
}

func TestMustFromContext(t *testing.T) {
	ctx := t.Context()
	ctx = sequence.With(WithFlow(ctx, running(t, NewFlowExecution())), DefaultReplayFlags)
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
