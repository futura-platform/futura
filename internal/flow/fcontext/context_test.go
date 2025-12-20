package fcontext

import (
	"context"
	"errors"
	"testing"

	"github.com/futura-platform/futura/ftype"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/petermattis/goid"
	"github.com/stretchr/testify/assert"
)

func TestWithFlow(t *testing.T) {
	ctx := context.Background()
	opts := []ftype.FlowLoopOption{func(flo *ftype.FlowLoopOptions) {}}
	ctx = WithFlow(ctx, opts)
	flowContext, ok := FromContext(ctx)
	assert.True(t, ok)
	assert.NotNil(t, flowContext)
	assert.Equal(t, opts, flowContext.Options())

	ctx2 := context.Background()
	flowContext2, ok := FromContext(ctx2)
	assert.False(t, ok)
	assert.Nil(t, flowContext2)
}

func TestGetFlowContext_WrongGoroutine(t *testing.T) {
	ctx := context.Background()
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
	ctx := context.Background()
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
