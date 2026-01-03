package fcontext

import (
	"errors"
	"reflect"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/futura/ftype"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/moment"
	"github.com/petermattis/goid"
	"github.com/stretchr/testify/assert"
)

func TestWithFlow(t *testing.T) {
	ctx := t.Context()
	opts := []ftype.FlowLoopOption{func(flo *ftype.FlowLoopOptions) {}}
	ctx = WithFlow(ctx, opts)
	flowContext, ok := FromContext(ctx)
	assert.True(t, ok)
	assert.NotNil(t, flowContext)
	assert.Equal(t, opts, flowContext.Options())

	ctx2 := t.Context()
	flowContext2, ok := FromContext(ctx2)
	assert.False(t, ok)
	assert.Nil(t, flowContext2)
}

func TestGetFlowContext_WrongGoroutine(t *testing.T) {
	ctx := t.Context()
	ctx = WithFlow(ctx, []ftype.FlowLoopOption{})
	flowContext, _ := FromContext(ctx)
	t.Run("panics", func(t *testing.T) {
		expectedError := ftrerrors.InconsistentStateError(FlowContextUsedInWrongGoroutineError{
			createdInGoroutineID: flowContext.creatorGoroutineID,
			usedInGoroutineID:    goid.Get(),
		})
		assert.PanicsWithError(t, expectedError.Error(), func() {
			FromContext(ctx)
		})
	})
}

func TestCancelCurrentReplay(t *testing.T) {
	ctx := t.Context()
	ctx = WithFlow(ctx, []ftype.FlowLoopOption{})
	flowContext := MustFromContext(ctx)
	assert.PanicsWithError(t, ftrerrors.InconsistentStateError(ErrNoCurrentReplay).Error(), func() {
		flowContext.RestartCurrentReplay(ctx, errors.New("placeholder"))
	})

	flowContext.cancelCurrentReplay = func(cause error) {}
	assert.PanicsWithError(t, ftrerrors.InconsistentStateError(ErrNilCancellationCause).Error(), func() {
		flowContext.RestartCurrentReplay(ctx, nil)
	})
}

func TestExpectedIdentity(t *testing.T) {
	t.Run("has expected call", func(t *testing.T) {
		ctx := t.Context()
		ctx = WithFlow(ctx, []ftype.FlowLoopOption{})
		flowContext := MustFromContext(ctx)

		flowContext.callOrder = make([]moment.Identity, 1)
		flowContext.Rewind()

		identity, ok := flowContext.ExpectedIdentity()
		assert.True(t, ok)
		assert.Equal(t, flowContext.callOrder[0], identity)
	})
	t.Run("no expected call", func(t *testing.T) {
		ctx := t.Context()
		ctx = WithFlow(ctx, []ftype.FlowLoopOption{})
		flowContext := MustFromContext(ctx)

		flowContext.callOrder = make([]moment.Identity, 0)
		flowContext.Rewind()

		_, ok := flowContext.ExpectedIdentity()
		assert.False(t, ok)
	})
	t.Run("sequence index out of bounds", func(t *testing.T) {
		ctx := t.Context()
		ctx = WithFlow(ctx, []ftype.FlowLoopOption{})
		flowContext := MustFromContext(ctx)

		flowContext.callOrder = make([]moment.Identity, 2)
		flowContext.sequenceIndex = 10

		assert.PanicsWithError(t, ftrerrors.InconsistentStateError(SequenceIndexOutOfBoundsError{
			sequenceIndex:  10,
			sequenceLength: 2,
		}).Error(), func() {
			flowContext.ExpectedIdentity()
		})
	})
}

func TestResetUnseenCachedCallpaths(t *testing.T) {
	ctx := t.Context()
	ctx = WithFlow(ctx, []ftype.FlowLoopOption{})
	flowContext := MustFromContext(ctx)
	flowContext.resetUnseenCachedCallpaths()
	assert.True(t, flowContext.unseenCachedCallpaths.IsEmpty())

	cachedIdentity := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}})
	flowContext.stateCache = map[moment.Identity]*moment.Moment{
		cachedIdentity: nil,
	}
	flowContext.resetUnseenCachedCallpaths()
	assert.True(t, flowContext.unseenCachedCallpaths.Contains(cachedIdentity))
}

func TestReplayFlags(t *testing.T) {
	ctx := t.Context()
	ctx = WithFlow(ctx, []ftype.FlowLoopOption{})
	flowContext := MustFromContext(ctx)
	flowContext.SetReplayFlags(func(flags *ReplayFlags) {
		flags.PanicOnMomentOrderChange = true
	})
	assert.True(t, flowContext.ReplayFlags().PanicOnMomentOrderChange)
}

func TestGetMoment(t *testing.T) {
	ctx := t.Context()
	ctx = WithFlow(ctx, []ftype.FlowLoopOption{})
	flowContext := MustFromContext(ctx)
	identity := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}})
	m := moment.NewMoment(nil, 1)
	flowContext.stateCache = map[moment.Identity]*moment.Moment{
		identity: m,
	}
	r, ok := flowContext.GetMoment(identity)
	assert.True(t, ok)
	assert.Equal(t, r, m)
	_, ok = flowContext.GetMoment(moment.Identity{})
	assert.False(t, ok)
}

func TestAdvance(t *testing.T) {
	ctx := t.Context()
	ctx = WithFlow(ctx, []ftype.FlowLoopOption{})
	flowContext := MustFromContext(ctx)
	for i := range 10 {
		flowContext.Advance()
		assert.Equal(t, i+1, flowContext.SequenceIndex())
	}
}

func TestEvictUnseenCachedStates(t *testing.T) {
	ctx := t.Context()
	ctx = WithFlow(ctx, []ftype.FlowLoopOption{})
	flowContext := MustFromContext(ctx)
	toEvict := moment.NewIdentity(ctx, moment.Callpath{{File: "toEvict"}})
	toKeep := moment.NewIdentity(ctx, moment.Callpath{{File: "toKeep"}})
	flowContext.stateCache = map[moment.Identity]*moment.Moment{
		toEvict: nil,
		toKeep:  nil,
	}
	flowContext.unseenCachedCallpaths = mapset.NewSet(toEvict)
	flowContext.EvictUnseenCachedStates(ctx)

	assert.Equal(t, map[moment.Identity]*moment.Moment{
		toKeep: nil,
	}, flowContext.stateCache)
}

func TestRestartCurrentReplay(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		ctx := t.Context()
		ctx = WithFlow(ctx, []ftype.FlowLoopOption{})
		flowContext := MustFromContext(ctx)

		cancelCauses := make([]error, 0, 1)
		flowContext.cancelCurrentReplay = func(cause error) {
			cancelCauses = append(cancelCauses, cause)
		}
		cancelCause := errors.New("placeholder")
		flowContext.RestartCurrentReplay(ctx, cancelCause)

		assert.Equal(t, 1, len(cancelCauses))
		assert.ErrorIs(t, cancelCauses[0], ErrRestartReplay)
		assert.ErrorIs(t, cancelCauses[0], cancelCause)
	})
	t.Run("no cancel current replay case", func(t *testing.T) {
		ctx := t.Context()
		ctx = WithFlow(ctx, []ftype.FlowLoopOption{})
		flowContext := MustFromContext(ctx)
		assert.PanicsWithError(t, ftrerrors.InconsistentStateError(ErrNoCurrentReplay).Error(), func() {
			flowContext.RestartCurrentReplay(ctx, nil)
		})
	})
	t.Run("no cancel cause case", func(t *testing.T) {
		ctx := t.Context()
		ctx = WithFlow(ctx, []ftype.FlowLoopOption{})
		flowContext := MustFromContext(ctx)
		flowContext.cancelCurrentReplay = func(cause error) {}
		assert.PanicsWithError(t, ftrerrors.InconsistentStateError(ErrNilCancellationCause).Error(), func() {
			flowContext.RestartCurrentReplay(ctx, nil)
		})
	})
}

func TestStartNewReplay(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		ctx := t.Context()
		ctx = WithFlow(ctx, []ftype.FlowLoopOption{})
		flowContext := MustFromContext(ctx)
		replayCtx, cancel := flowContext.StartNewReplay(ctx)
		assert.NotNil(t, replayCtx)
		assert.NotNil(t, cancel)
		assert.True(t, flowContext.unseenCachedCallpaths.IsEmpty())
		assert.Equal(t, reflect.ValueOf(flowContext.cancelCurrentReplay).Pointer(), reflect.ValueOf(cancel).Pointer())
	})
}

func TestRecordCurrentMoment(t *testing.T) {
	t.Run("fresh moment case", func(t *testing.T) {
		ctx := WithFlow(t.Context(), []ftype.FlowLoopOption{})
		flowContext := MustFromContext(ctx)

		recordKey := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}})
		recordMoment := moment.NewMoment(nil, 1)

		flowContext.stateCache = make(map[moment.Identity]*moment.Moment)
		flowContext.resetUnseenCachedCallpaths()

		flowContext.RecordCurrentMoment(recordKey, recordMoment)
		assert.Equal(t, flowContext.callOrder[0], recordKey)
		assert.Equal(t, flowContext.SequenceIndex(), 0)
		assert.Equal(t, flowContext.stateCache[recordKey], recordMoment)
	})
	t.Run("existing moment case", func(t *testing.T) {
		ctx := WithFlow(t.Context(), []ftype.FlowLoopOption{})
		flowContext := MustFromContext(ctx)

		recordKey := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}})
		recordMoment := moment.NewMoment(nil, 1)

		flowContext.stateCache = map[moment.Identity]*moment.Moment{
			recordKey: recordMoment,
		}
		flowContext.callOrder = []moment.Identity{recordKey}
		flowContext.resetUnseenCachedCallpaths()
		assert.True(t, flowContext.unseenCachedCallpaths.Contains(recordKey))

		flowContext.RecordCurrentMoment(recordKey, recordMoment)
		assert.False(t, flowContext.unseenCachedCallpaths.Contains(recordKey))
		assert.Equal(t, flowContext.callOrder[0], recordKey)
		assert.Equal(t, flowContext.sequenceIndex, 0)
		assert.Equal(t, flowContext.stateCache[recordKey], recordMoment)
	})
	t.Run("unexpected cached state case", func(t *testing.T) {
		ctx := WithFlow(t.Context(), []ftype.FlowLoopOption{})
		flowContext := MustFromContext(ctx)

		recordKey := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}})
		recordMoment := moment.NewMoment(nil, 1)

		flowContext.stateCache = map[moment.Identity]*moment.Moment{
			recordKey: recordMoment,
		}
		flowContext.resetUnseenCachedCallpaths()
		assert.True(t, flowContext.unseenCachedCallpaths.Contains(recordKey))

		assert.PanicsWithError(t, ftrerrors.InconsistentStateError(UnexpectedCachedStateError{
			identity: recordKey,
		}).Error(), func() {
			flowContext.RecordCurrentMoment(recordKey, recordMoment)
		})
	})
	t.Run("sequence index out of bounds", func(t *testing.T) {
		ctx := t.Context()
		ctx = WithFlow(ctx, []ftype.FlowLoopOption{})
		flowContext := MustFromContext(ctx)

		flowContext.sequenceIndex = 10
		flowContext.callOrder = make([]moment.Identity, 2)

		assert.PanicsWithError(t, ftrerrors.InconsistentStateError(SequenceIndexOutOfBoundsError{
			sequenceIndex:  10,
			sequenceLength: 2,
		}).Error(), func() {
			flowContext.RecordCurrentMoment(moment.Identity{}, nil)
		})
	})
}

func TestInvalidateMoment(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		ctx := t.Context()
		ctx = WithFlow(ctx, []ftype.FlowLoopOption{})
		flowContext := MustFromContext(ctx)

		key := moment.NewIdentity(ctx, moment.Callpath{{File: "placeholder"}})
		flowContext.stateCache = map[moment.Identity]*moment.Moment{
			key: nil,
		}
		flowContext.InvalidateMoment(key)
		assert.Equal(t, map[moment.Identity]*moment.Moment{}, flowContext.stateCache)
	})
}

func TestMustFromContext(t *testing.T) {
	ctx := t.Context()
	ctx = WithFlow(ctx, []ftype.FlowLoopOption{})
	flowContext := MustFromContext(ctx)
	assert.NotNil(t, flowContext)

	ctx2 := t.Context()
	assert.PanicsWithError(t, ErrFlowContextNotFound.Error(), func() {
		MustFromContext(ctx2)
	})
}
