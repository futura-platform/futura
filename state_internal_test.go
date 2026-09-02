package futura

import (
	"context"
	"testing"

	"github.com/futura-platform/futura/internal/durable"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/flow/replay"
	"github.com/futura-platform/futura/internal/step"
	"github.com/futura-platform/futura/internal/utils/containertest"
	"github.com/futura-platform/futura/internal/utils/testutil"
	"github.com/stretchr/testify/assert"
)

func TestState_InternalErrorPaths(t *testing.T) {
	t.Run("panics when state is evaluated outside of a replay flow function", func(t *testing.T) {
		exec := execution.NewFlowExecutionWithContainer(containertest.NewInMemory())
		startExecRun(t, exec)
		ctx := durable.WithHandlesCache()(execution.WithFlow(t.Context(), exec))
		b := stateContext.Provide(newFlowBuilder(ctx, exec))

		testutil.PanicsWithErrorIs(t, step.ErrEvaledOutsideOfAFlowFunction, func() {
			_ = stateWithInitialValue(b, 1)
		})
	})

	t.Run("panics when state key is missing from the durable state map", func(t *testing.T) {
		exec := execution.NewFlowExecutionWithContainer(containertest.NewInMemory())
		startExecRun(t, exec)
		ctx := durable.WithHandlesCache()(execution.WithFlow(t.Context(), exec))
		ctx, _ = exec.StartNewReplay(ctx)

		_, err := replay.Execute(ctx, func(ctx context.Context, _ struct{}) (struct{}, error) {
			b := stateContext.Provide(newFlowBuilder(ctx, exec))
			state := stateWithInitialValue(b, 1)

			// Force an inconsistent durable map to exercise the ErrStateNotFound panic path.
			values, _ := stateContext.Use(b)
			values.mu.Lock()
			values.values = map[string][]byte{}
			values.mu.Unlock()

			testutil.PanicsWithErrorIs(t, ErrStateNotFound, func() {
				_ = state.V()
			})

			return struct{}{}, nil
		}, struct{}{})
		assert.NoError(t, err)
	})
}
