package execution

import (
	"context"
	"errors"
	"testing"

	"github.com/futura-platform/futura/ftype/executiontype"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/replay"
	"github.com/futura-platform/futura/internal/flow/replay/sequence"
	"github.com/futura-platform/futura/internal/goroutinebind"
	"github.com/futura-platform/futura/moment"
	"github.com/petermattis/goid"
	"github.com/stretchr/testify/assert"
)

func TestWithFlow(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		ctx := t.Context()
		fOriginal := NewFlowExecution()
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
	ctx = WithFlow(ctx, NewFlowExecution())
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
	ctx = WithFlow(ctx, NewFlowExecution())
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
		ctx = sequence.With(WithFlow(ctx, NewFlowExecutionWithContainer(c)), DefaultReplayFlags)
		f := MustFromContext(ctx)

		c.AppendCallOrder(moment.Identity{})

		identity, ok := f.ExpectedIdentity(ctx)
		assert.True(t, ok)
		assert.Equal(t, c.CallOrderAt(0), identity)
	})
	t.Run("no expected call", func(t *testing.T) {
		ctx := t.Context()
		c := executiontype.NewInMemoryContainer()
		ctx = sequence.With(WithFlow(ctx, NewFlowExecutionWithContainer(c)), DefaultReplayFlags)
		f := MustFromContext(ctx)

		_, ok := f.ExpectedIdentity(ctx)
		assert.False(t, ok)
	})
	t.Run("sequence index out of bounds", func(t *testing.T) {
		ctx := t.Context()
		c := executiontype.NewInMemoryContainer()
		ctx = sequence.With(WithFlow(ctx, NewFlowExecutionWithContainer(c)), DefaultReplayFlags)
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
	ctx = sequence.With(WithFlow(ctx, NewFlowExecutionWithContainer(c)), DefaultReplayFlags)
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

func TestEvictUnseenCachedStates(t *testing.T) {
	ctx := t.Context()
	c := executiontype.NewInMemoryContainer()
	ctx = sequence.With(WithFlow(ctx, NewFlowExecutionWithContainer(c)), DefaultReplayFlags)
	f := MustFromContext(ctx)
	toEvict := moment.NewIdentity(ctx, moment.Callpath{{File: "toEvict"}})
	toKeep := moment.NewIdentity(ctx, moment.Callpath{{File: "toKeep"}})
	c.SetMoment(toEvict, moment.Moment{})
	c.SetMoment(toKeep, moment.Moment{})
	sequence.MarkSeen(ctx, toKeep)
	f.EvictUnseenCachedMoments(ctx)

	assert.False(t, c.HasMoment(toEvict))
	assert.True(t, c.HasMoment(toKeep))
}

func TestRestartCurrentReplay(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		ctx := t.Context()
		ctx = sequence.With(WithFlow(ctx, NewFlowExecution()), DefaultReplayFlags)
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
		ctx = sequence.With(WithFlow(ctx, NewFlowExecution()), DefaultReplayFlags)
		f := MustFromContext(ctx)
		assert.PanicsWithError(t, ftrerrors.InconsistentStateError(ErrNoCurrentReplay).Error(), func() {
			f.RestartCurrentReplay(ctx, nil)
		})
	})
	t.Run("no cancel cause case", func(t *testing.T) {
		ctx := t.Context()
		ctx = sequence.With(WithFlow(ctx, NewFlowExecution()), DefaultReplayFlags)
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
		ctx = sequence.With(WithFlow(ctx, NewFlowExecution()), DefaultReplayFlags)
		f := MustFromContext(ctx)
		replayCtx := f.StartNewReplay(ctx)
		assert.True(t, replay.Has(replayCtx))
	})
}

func TestRecordCurrentMoment(t *testing.T) {
	t.Run("fresh moment case", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		ctx := sequence.With(WithFlow(t.Context(), NewFlowExecutionWithContainer(c)), DefaultReplayFlags)
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
		ctx := sequence.With(WithFlow(t.Context(), NewFlowExecutionWithContainer(c)), DefaultReplayFlags)
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
		ctx := WithFlow(t.Context(), NewFlowExecutionWithContainer(c))
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
		ctx = sequence.With(WithFlow(ctx, NewFlowExecutionWithContainer(c)), DefaultReplayFlags)
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
	ctx = sequence.With(WithFlow(ctx, NewFlowExecution()), DefaultReplayFlags)
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
