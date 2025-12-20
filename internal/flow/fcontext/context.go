package fcontext

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/futura/flog"
	"github.com/futura-platform/futura/ftype"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/moment"
	"github.com/petermattis/goid"
)

type ctxKey string

const flowContextKey ctxKey = "futura_flow"

type ReplayFlags struct {
	PanicOnMomentOrderChange bool
}
type flowContext struct {
	options            []ftype.FlowLoopOption
	replayFlags        ReplayFlags
	creatorGoroutineID int64

	cancelCurrentReplay context.CancelCauseFunc

	// the current state of the flow.
	stateCache map[ftype.Sealed[moment.Callpath]]*moment.Moment
	// this includes the callpaths that have not been seen yet in the current replay.
	unseenCachedCallpaths mapset.Set[ftype.Sealed[moment.Callpath]]

	// given a certain state, this sequence should be deterministic.
	callOrder     []ftype.Sealed[moment.Callpath]
	sequenceIndex int
}

func WithFlow(ctx context.Context, options []ftype.FlowLoopOption) context.Context {
	return context.WithValue(ctx, flowContextKey, &flowContext{
		options:            options,
		creatorGoroutineID: goid.Get(),
		stateCache:         make(map[ftype.Sealed[moment.Callpath]]*moment.Moment),
	})
}

func (f *flowContext) Options() []ftype.FlowLoopOption {
	return f.options
}

func (f *flowContext) Rewind() {
	f.sequenceIndex = 0
}

func (f *flowContext) SequenceIndex() int {
	return f.sequenceIndex
}

func (f *flowContext) EvictUnseenCachedStates(ctx context.Context) {
	l := flog.FromContext(ctx)
	for callpath := range mapset.Elements(f.unseenCachedCallpaths) {
		delete(f.stateCache, callpath)
		l.LogAttrs(ctx, slog.LevelDebug, "evicted cached state",
			slog.String("callpath", callpath.V().String()),
		)
	}
}

var (
	ErrNoCurrentReplay      = errors.New("no current replay")
	ErrNilCancellationCause = errors.New("current replay cancellation cause cannot be nil")

	ErrRestartReplay = errors.New("restarting replay")
)

// RestartCurrentReplay cancels the current replay, which will always start a new one.
// (Regardless of whether or not the last replay was successful).
// This will also skip the default end of replay behavior, including rewinding the sequence index and resetting the replay flags.
func (f *flowContext) RestartCurrentReplay(ctx context.Context, cause error) {
	if f.cancelCurrentReplay == nil {
		panic(ftrerrors.InconsistentStateError(ErrNoCurrentReplay))
	} else if cause == nil {
		panic(ftrerrors.InconsistentStateError(ErrNilCancellationCause))
	}
	l := flog.FromContext(ctx)
	l.LogAttrs(ctx, slog.LevelDebug, "restarting replay",
		slog.String("cause", cause.Error()),
	)
	f.cancelCurrentReplay(fmt.Errorf("%w: %w", ErrRestartReplay, cause))
}

func (f *flowContext) StartNewReplay(ctx context.Context) (context.Context, context.CancelCauseFunc) {
	f.resetUnseenCachedCallpaths()
	replayCtx, cancel := context.WithCancelCause(ctx)
	f.cancelCurrentReplay = cancel
	return replayCtx, cancel
}

func (f *flowContext) resetUnseenCachedCallpaths() {
	f.unseenCachedCallpaths = mapset.NewSetWithSize[ftype.Sealed[moment.Callpath]](len(f.stateCache))
	for callpath := range f.stateCache {
		f.unseenCachedCallpaths.Add(callpath)
	}
}

func (f *flowContext) ReplayFlags() ReplayFlags {
	return f.replayFlags
}

func (f *flowContext) SetReplayFlags(flags func(flags *ReplayFlags)) {
	flags(&f.replayFlags)
}

func (f *flowContext) ExpectedCallpath() (ftype.Sealed[moment.Callpath], bool) {
	if f.sequenceIndex > len(f.callOrder) {
		panic(ftrerrors.InconsistentStateError(SequenceIndexOutOfBoundsError{
			sequenceIndex:  f.sequenceIndex,
			sequenceLength: len(f.callOrder),
		}))
	} else if f.sequenceIndex == len(f.callOrder) {
		return nil, false
	}

	callsite := f.callOrder[f.sequenceIndex]
	return callsite, true
}

func (f *flowContext) GetMoment(callpath ftype.Sealed[moment.Callpath]) (*moment.Moment, bool) {
	moment, ok := f.stateCache[callpath]
	return moment, ok
}

func (f *flowContext) InvalidateMoment(callpath ftype.Sealed[moment.Callpath]) {
	delete(f.stateCache, callpath)
}

// RecordCurrentMoment stores the current callpath+moment (growing the sequence slice if necessary)
// it also marks the callpath as seen.
func (f *flowContext) RecordCurrentMoment(callpath ftype.Sealed[moment.Callpath], currentMoment *moment.Moment) {
	if f.sequenceIndex > len(f.callOrder) {
		panic(ftrerrors.InconsistentStateError(SequenceIndexOutOfBoundsError{
			sequenceIndex:  f.sequenceIndex,
			sequenceLength: len(f.callOrder),
		}))
	} else if f.sequenceIndex == len(f.callOrder) {
		f.callOrder = append(f.callOrder, callpath)
	} else {
		f.callOrder[f.sequenceIndex] = callpath
	}
	f.unseenCachedCallpaths.Remove(callpath)
	f.stateCache[callpath] = currentMoment
}

// Advance advances the sequence index by 1.
func (f *flowContext) Advance() {
	f.sequenceIndex++
}

// FromContext retrieves the flow context from the context.
// It will panic if this is being used in a goroutine other than the one that created the context.
func FromContext(ctx context.Context) (*flowContext, bool) {
	v, ok := ctx.Value(flowContextKey).(*flowContext)
	if ok && v.creatorGoroutineID != goid.Get() {
		panic(ftrerrors.InconsistentStateError(FlowContextUsedInWrongGoroutineError{
			createdInGoroutineID: v.creatorGoroutineID,
			usedInGoroutineID:    goid.Get(),
		}))
	}
	return v, ok
}

func MustFromContext(ctx context.Context) *flowContext {
	f, ok := FromContext(ctx)
	if !ok {
		panic("flowContext not found in context")
	}
	return f
}

type SequenceIndexOutOfBoundsError struct {
	sequenceIndex  int
	sequenceLength int
}

func (e SequenceIndexOutOfBoundsError) Error() string {
	return fmt.Sprintf("sequenceIndex is greater than the length of the memoized moment sequence: %d > %d", e.sequenceIndex, e.sequenceLength)
}

type FlowContextUsedInWrongGoroutineError struct {
	createdInGoroutineID int64
	usedInGoroutineID    int64
}

func (e FlowContextUsedInWrongGoroutineError) Error() string {
	return fmt.Sprintf("flowContext created in goroutine %d was used in goroutine %d", e.createdInGoroutineID, e.usedInGoroutineID)
}
