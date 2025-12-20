package step

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/futura-platform/futura/flog"
	"github.com/futura-platform/futura/ftype"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/fcontext"
	"github.com/futura-platform/futura/internal/flow/moment"
	"github.com/futura-platform/futura/internal/flow/replay"
)

var (
	ErrEvaledOutsideOfAFlowFunction = errors.New("steps cannot be evaluated outside of a replay function")
	ErrUnexpectedBranchTaken        = errors.New("unexpected branch taken, new branches should only be triggered by a futura state change")
)

func Evaluate[A comparable, R comparable](ctx context.Context, fn moment.Fn[A, R], args A) (output R, invalidate func(), err error) {
	callpath, ok := replay.GetClosestReplayUserCallpath(0)
	if !ok {
		panic(ftrerrors.InconsistentStateError(ErrEvaledOutsideOfAFlowFunction))
	}

	return evaluateWithCallsite(ctx, fn, args, callpath)
}

func evaluateWithCallsite[A comparable, R comparable](ctx context.Context, fn moment.Fn[A, R], args A, callpath moment.Callpath) (output R, invalidate func(), err error) {
	if ctx.Err() != nil {
		err = ctx.Err()
		return
	}
	f := fcontext.MustFromContext(ctx)
	l := flog.FromContext(ctx)

	thisSequenceIndex := f.SequenceIndex()
	var cacheStatus string = "MISS"
	defer func() {
		r := recover()
		l.LogAttrs(ctx, slog.LevelDebug, "evaluated step",
			slog.String("label", fn.Label()),
			slog.Bool("success", err == nil),
			slog.Int("index", thisSequenceIndex),
			slog.String("cache_status", cacheStatus),
		)
		if r != nil {
			panic(r)
		}
	}()

	sealedCallpath := ftype.Seal(callpath)
	// first check if the expected callpath is the same as the current callpath,
	// if nothing is expected (meaning if ok is false), we can continue
	d := sealedCallpath.V()
	expectedCallpath, ok := f.ExpectedCallpath()
	if ok {
		dd := expectedCallpath.V()
		fmt.Println(d, dd, slices.Equal(d, dd))
	}
	if ok && expectedCallpath != sealedCallpath && f.ReplayFlags().PanicOnMomentOrderChange {
		panic(ftrerrors.InconsistentStateError(ErrUnexpectedBranchTaken))
	}

	// then get the moment from the cache
	currentMoment, ok := f.GetMoment(sealedCallpath)
	if !ok {
		currentMoment = moment.NewMoment(fn, args)
	}

	// validate BEFORE deferring the output handler, so that in the event of a panic, nothing is recorded.
	needsExecution := !ok || !currentMoment.Validate(thisSequenceIndex, fn, args, callpath)
	defer func() {
		// handle the result of the step
		f.RecordCurrentMoment(sealedCallpath, currentMoment)
		if err != nil {
			currentMoment.Invalidate()
			return
		}
		f.Advance()
	}()
	// validate it. If it no longer valid, re execute it and update the cache
	if needsExecution {
		output, err = fn.Call(ctx, args)
		if err != nil {
			return
		}
		currentMoment.SetOutput(output)
	} else {
		cacheStatus = "HIT"
	}

	// setup invalidation function
	invalidate = func() { f.InvalidateMoment(sealedCallpath) }

	// return the memoized result
	anyOutput := currentMoment.Output()
	if anyOutput == nil {
		return
	}
	return anyOutput.(R), invalidate, nil
}
