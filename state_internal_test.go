package futura

import (
	"testing"

	"github.com/futura-platform/futura/internal/durable"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/step"
	"github.com/futura-platform/futura/internal/utils/containertest"
	"github.com/futura-platform/futura/internal/utils/testutil"
)

func TestState_InternalErrorPaths(t *testing.T) {
	t.Run("panics when state is evaluated outside of a replay flow function", func(t *testing.T) {
		exec := execution.NewFlowExecutionWithContainer(containertest.NewInMemory())
		startExecRun(t, exec)
		ctx := durable.WithHandlesCache()(execution.WithFlow(t.Context(), exec))
		b := newFlowBuilder(ctx, exec)

		testutil.PanicsWithErrorIs(t, step.ErrEvaledOutsideOfAFlowFunction, func() {
			_ = stateWithInitialValue(b, 1)
		})
	})
}
