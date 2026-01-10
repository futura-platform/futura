package execution

import (
	"context"
	"errors"
	"testing"

	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/moment"
	"github.com/futura-platform/futura/internal/flow/replay"
	"github.com/futura-platform/futura/internal/flow/replay/sequence"
	"github.com/futura-platform/futura/internal/goroutinebind"
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
		ctx = sequence.With(WithFlow(ctx, NewFlowExecution()), DefaultReplayFlags)
		f := MustFromContext(ctx)

		f.s.callOrder = make([]moment.Identity, 1)

		identity, ok := f.ExpectedIdentity(ctx)
		assert.True(t, ok)
		assert.Equal(t, f.s.callOrder[0], identity)
	})
	t.Run("no expected call", func(t *testing.T) {
		ctx := t.Context()
		ctx = sequence.With(WithFlow(ctx, NewFlowExecution()), DefaultReplayFlags)
		f := MustFromContext(ctx)

		f.s.callOrder = make([]moment.Identity, 0)

		_, ok := f.ExpectedIdentity(ctx)
		assert.False(t, ok)
	})
	t.Run("sequence index out of bounds", func(t *testing.T) {
		ctx := t.Context()
		ctx = sequence.With(WithFlow(ctx, NewFlowExecution()), DefaultReplayFlags)
		f := MustFromContext(ctx)

		f.s.callOrder = make([]moment.Identity, 2)
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
	ctx = WithFlow(ctx, NewFlowExecution())
	f := MustFromContext(ctx)
	identity := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}})
	m := moment.NewMoment(placeholderCallable, 1)
	f.s.stateCache = map[moment.Identity]*moment.Moment{
		identity: m,
	}
	r, ok := f.GetMoment(identity)
	assert.True(t, ok)
	assert.Equal(t, r, m)
	_, ok = f.GetMoment(moment.Identity{})
	assert.False(t, ok)
}

func TestEvictUnseenCachedStates(t *testing.T) {
	ctx := t.Context()
	ctx = sequence.With(WithFlow(ctx, NewFlowExecution()), DefaultReplayFlags)
	f := MustFromContext(ctx)
	toEvict := moment.NewIdentity(ctx, moment.Callpath{{File: "toEvict"}})
	toKeep := moment.NewIdentity(ctx, moment.Callpath{{File: "toKeep"}})
	f.s.stateCache = map[moment.Identity]*moment.Moment{
		toEvict: nil,
		toKeep:  nil,
	}
	sequence.MarkSeen(ctx, toKeep)
	f.EvictUnseenCachedStates(ctx)

	assert.Equal(t, map[moment.Identity]*moment.Moment{
		toKeep: nil,
	}, f.s.stateCache)
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
		ctx := sequence.With(WithFlow(t.Context(), NewFlowExecution()), DefaultReplayFlags)
		f := MustFromContext(ctx)

		recordKey := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}})
		recordMoment := moment.NewMoment(placeholderCallable, 1)

		f.s.stateCache = make(map[moment.Identity]*moment.Moment)

		f.RecordCurrentMoment(ctx, recordKey, recordMoment)
		assert.Equal(t, f.s.callOrder[0], recordKey)
		assert.Equal(t, sequence.GetIndex(ctx), 0)
		assert.Equal(t, f.s.stateCache[recordKey], recordMoment)
	})
	t.Run("existing moment case", func(t *testing.T) {
		ctx := sequence.With(WithFlow(t.Context(), NewFlowExecution()), DefaultReplayFlags)
		f := MustFromContext(ctx)

		recordKey := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}})
		recordMoment := moment.NewMoment(placeholderCallable, 1)

		f.s.stateCache = map[moment.Identity]*moment.Moment{
			recordKey: recordMoment,
		}
		f.s.callOrder = []moment.Identity{recordKey}
		assert.False(t, sequence.IsSeen(ctx, recordKey))

		f.RecordCurrentMoment(ctx, recordKey, recordMoment)
		assert.True(t, sequence.IsSeen(ctx, recordKey))
		assert.Equal(t, f.s.callOrder[0], recordKey)
		assert.Equal(t, sequence.GetIndex(ctx), 0)
		assert.Equal(t, f.s.stateCache[recordKey], recordMoment)
	})
	t.Run("unexpected cached state case", func(t *testing.T) {
		ctx := sequence.With(WithFlow(t.Context(), NewFlowExecution()), DefaultReplayFlags)
		f := MustFromContext(ctx)

		recordKey := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}})
		recordMoment := moment.NewMoment(placeholderCallable, 1)

		f.s.stateCache = map[moment.Identity]*moment.Moment{
			recordKey: recordMoment,
		}
		assert.False(t, sequence.IsSeen(ctx, recordKey))

		assert.PanicsWithError(t, ftrerrors.InconsistentStateError(UnexpectedCachedStateError{
			identity: recordKey,
		}).Error(), func() {
			f.RecordCurrentMoment(ctx, recordKey, recordMoment)
		})
	})
	t.Run("sequence index out of bounds", func(t *testing.T) {
		ctx := t.Context()
		ctx = sequence.With(WithFlow(ctx, NewFlowExecution()), DefaultReplayFlags)
		f := MustFromContext(ctx)

		ctx = sequence.With(ctx, DefaultReplayFlags)
		for range 10 {
			sequence.Advance(ctx)
		}
		f.s.callOrder = make([]moment.Identity, 2)

		assert.PanicsWithError(t, ftrerrors.InconsistentStateError(SequenceIndexOutOfBoundsError{
			sequenceIndex:  10,
			sequenceLength: 2,
		}).Error(), func() {
			f.RecordCurrentMoment(ctx, moment.Identity{}, nil)
		})
	})
}

func TestInvalidateMoment(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		ctx := t.Context()
		ctx = sequence.With(WithFlow(ctx, NewFlowExecution()), DefaultReplayFlags)
		f := MustFromContext(ctx)

		key := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}})
		f.s.stateCache = map[moment.Identity]*moment.Moment{
			key: nil,
		}
		f.InvalidateMoment(key)
		assert.Equal(t, map[moment.Identity]*moment.Moment{}, f.s.stateCache)
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
	s := FlowExecutionState{
		stateCache: make(map[moment.Identity]*moment.Moment),
	}
	f := NewFlowExecutionFromState(s)
	assert.NotNil(t, f)
	assert.Equal(t, s, f.s)
}
