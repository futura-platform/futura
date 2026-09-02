package step

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"runtime"

	"github.com/futura-platform/futura/flog"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/flow/replay"
	"github.com/futura-platform/futura/internal/flow/replay/sequence"
	"github.com/futura-platform/futura/internal/utils/testutil"
	"github.com/futura-platform/futura/moment"
	"github.com/futura-platform/futura/privateencoding"
	"k8s.io/utils/diff"
)

var (
	ErrEvaledOutsideOfAFlowFunction = errors.New("steps cannot be evaluated outside of a replay function")
	ErrUnexpectedBranchTaken        = errors.New("unexpected branch taken, new branches should only be triggered by a futura state change")
)

func init() {
	// Evaluate is the single point at which user callstacks are captured for moment identities.
	replay.SetCaptureFunction(Evaluate[struct{}, struct{}])
}

func Evaluate[A comparable, R any](
	ctx context.Context,
	fn moment.Fn[A, R],
	args A,
) (output R, err error) {
	if err = testutil.InjectedError(ctx, testutil.InjectedErrorLevelEvaluate); err != nil {
		return
	}

	callstack, ok := replay.GetClosestReplayUserCallstack()
	if !ok {
		panic(ftrerrors.InconsistentStateError(ErrEvaledOutsideOfAFlowFunction))
	}

	return evaluateWithCallstack(
		ctx,
		fn,
		args,
		callstack,
	)
}

var ErrEvalFailed = errors.New("eval failed")

func evaluateWithCallstack[A comparable, R any](
	ctx context.Context,
	fn moment.Fn[A, R],
	args A,
	callstack []runtime.Frame,
) (output R, err error) {
	if ctx.Err() != nil {
		err = ctx.Err()
		return
	}
	identity := moment.NewIdentity(
		ctx,
		replay.CallstackToCallpath(callstack),
	)

	f := execution.MustFromContext(ctx)
	l := flog.FromContext(ctx)

	thisSequenceIndex := sequence.GetIndex(ctx)
	var cacheStatus string = "MISS"
	defer func() {
		r := recover()
		l.LogAttrs(ctx, slog.LevelDebug, "evaluated step",
			slog.String("label", fn.Label()),
			slog.Int("index", thisSequenceIndex),
			slog.String("cache_status", cacheStatus),
			slog.String("error", fmt.Sprint(err)),
		)
		if r != nil {
			panic(r)
		} else if err != nil {
			err = fmt.Errorf("%s: %w: %w", fn.Label(), ErrEvalFailed, err)
		}
	}()

	// first check if the expected callpath is the same as the current callpath,
	// if nothing is expected (meaning if ok is false), we can continue
	expectedIdentity, ok := f.ExpectedIdentity(ctx)
	if ok && expectedIdentity.Callpath() != identity.Callpath() && sequence.GetFlags(ctx).PanicOnMomentOrderChange {
		panic(ftrerrors.InconsistentStateError(fmt.Errorf(
			"%w:\n%s",
			ErrUnexpectedBranchTaken,
			diff.ObjectGoPrintSideBySide(
				expectedIdentity.Callpath().V(),
				identity.Callpath().V(),
			),
		)))
	}

	// register input and output types so moments can be properly serialized/deserialized
	// (ignore for interfaces, we expect them to be registered by the caller)
	inputRt := reflect.TypeFor[A]()
	if inputRt.Kind() != reflect.Interface {
		privateencoding.RegisterType(inputRt)
	}
	outputRt := reflect.TypeFor[R]()
	if outputRt.Kind() != reflect.Interface {
		privateencoding.RegisterType(outputRt)
	}

	// then get the moment from the cache
	currentMomentValue, ok := f.GetMoment(ctx, identity)
	currentMoment := &currentMomentValue

	// validate BEFORE deferring the output handler, so that in the event of a panic, nothing is recorded.
	needsExecution := !ok || !currentMoment.Validate(thisSequenceIndex, fn, args, identity)
	if needsExecution {
		// the recorded moment (if any) is for a stale input, so record the execution against a fresh one.
		currentMoment = moment.NewMoment(fn, args)
	}
	defer func() {
		if err != nil {
			currentMoment.Invalidate()
		}
		// handle the result of the step, if it was successful
		f.RecordCurrentMoment(ctx, identity, *currentMoment)
		if err != nil {
			return
		}
		sequence.Advance(ctx)
	}()
	// validate it. If it no longer valid, re execute it and update the cache
	if needsExecution {
		output, err = call(ctx, fn, identity, args, callstack)
		if err != nil {
			return
		}
		currentMoment.SetValidOutput(output)
	} else {
		cacheStatus = "HIT"
	}

	// return the memoized result
	anyOutput, ok := currentMoment.Output().Get()
	if !ok {
		panic(ftrerrors.InconsistentStateError(fmt.Errorf("expected moment to have a valid output, but it was not")))
	}
	return anyOutput.(R), nil
}
