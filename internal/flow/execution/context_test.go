package execution

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/futura-platform/futura/ftype/executiontype"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/replay"
	"github.com/futura-platform/futura/internal/flow/replay/sequence"
	"github.com/futura-platform/futura/internal/goroutinebind"
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
	ctx := t.Context()
	ctx = WithFlow(ctx, running(t, NewFlowExecution()))
	f := MustFromContext(ctx)
	assert.PanicsWithError(t, ftrerrors.InconsistentStateError(ErrNoCurrentReplay).Error(), func() {
		f.RestartCurrentReplay(ctx, errors.New("placeholder"))
	})

	ctx = replay.With(ctx)
	assert.PanicsWithError(t, ftrerrors.InconsistentStateError(ErrNilCancellationCause).Error(), func() {
		f.RestartCurrentReplay(ctx, nil)
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
	t.Run("normal case", func(t *testing.T) {
		ctx := t.Context()
		ctx = sequence.With(WithFlow(ctx, running(t, NewFlowExecution())), DefaultReplayFlags)
		f := MustFromContext(ctx)

		ctx = replay.With(ctx)
		cancelCause := errors.New("placeholder")
		f.RestartCurrentReplay(ctx, cancelCause)

		cause := context.Cause(ctx)
		assert.ErrorIs(t, cause, ErrRestartReplay)
		assert.ErrorIs(t, cause, cancelCause)
	})
	t.Run("no cancel current replay case", func(t *testing.T) {
		ctx := t.Context()
		ctx = sequence.With(WithFlow(ctx, running(t, NewFlowExecution())), DefaultReplayFlags)
		f := MustFromContext(ctx)
		assert.PanicsWithError(t, ftrerrors.InconsistentStateError(ErrNoCurrentReplay).Error(), func() {
			f.RestartCurrentReplay(ctx, nil)
		})
	})
	t.Run("no cancel cause case", func(t *testing.T) {
		ctx := t.Context()
		ctx = sequence.With(WithFlow(ctx, running(t, NewFlowExecution())), DefaultReplayFlags)
		f := MustFromContext(ctx)
		ctx = replay.With(ctx)
		assert.PanicsWithError(t, ftrerrors.InconsistentStateError(ErrNilCancellationCause).Error(), func() {
			f.RestartCurrentReplay(ctx, nil)
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
	c := executiontype.NewInMemoryContainer()
	f := NewFlowExecutionWithContainer(c)
	ctx, _ := f.StartNewReplay(t.Context())

	getEpoch := func() uint64 {
		t.Helper()
		encoded, ok, err := c.LoadDurable(dirtyEpochKey)
		assert.NoError(t, err)
		assert.True(t, ok)
		var epoch uint64
		_, err = binary.Decode(encoded, binary.LittleEndian, &epoch)
		assert.NoError(t, err)
		return epoch
	}

	f.InvalidateSequence(ctx)
	assert.Equal(t, uint64(1), getEpoch())

	f.InvalidateSequence(ctx)
	assert.Equal(t, uint64(2), getEpoch())
}

func TestSettleSequence(t *testing.T) {
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
