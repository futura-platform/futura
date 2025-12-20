package replay

import (
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExecuteReplayLocation(t *testing.T) {
	var obervedExecuteReplayFrameFile string
	var obervedExecuteReplayFrameLine int
	Execute(t.Context(), func(ctx context.Context, args any) (any, error) {
		_, file, line, ok := runtime.Caller(1)
		if !ok {
			t.Fatal("failed to get caller")
		}
		obervedExecuteReplayFrameFile = file
		obervedExecuteReplayFrameLine = line
		return nil, nil
	}, nil)
	file, line := executeReplayCallFlowLocation()
	assert.Equal(t, obervedExecuteReplayFrameFile, file)
	assert.Equal(t, obervedExecuteReplayFrameLine, line)
}
