package replay

import (
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/futura-platform/futura/internal/flow/moment"
	"github.com/futura-platform/futura/internal/flow/replay/calliter"
)

func GetClosestReplayUserCallpath(skip int) (moment.Callpath, bool) {
	// targetFile, targetLine := executeReplayCallFlowLocation()
	var accumulatedCallpath moment.Callpath
	for frame := range calliter.NewFrameIter(8, skip+1) {
		if isExecuteReplayCallFlowFrame(frame) {
			// the callpath is built in reverse order, so we need to reverse it before returning
			slices.Reverse(accumulatedCallpath)
			return accumulatedCallpath, len(accumulatedCallpath) > 0
		} else if isFuturaFile(frame.File) && !isTestFile(frame.File) && !isFuturaExampleFile(frame.File) {
			// no need to record futura frames in the callpath
			continue
		}
		accumulatedCallpath = append(accumulatedCallpath, moment.Callsite{File: frame.File, Line: frame.Line})
	}
	return nil, false
}

func isTestFile(fileName string) bool {
	return strings.HasSuffix(fileName, "_test.go")
}

func isFuturaExampleFile(fileName string) bool {
	return strings.HasPrefix(fileName, filepath.Join(futuraModuleBasePath, "examples"))
}

func isFuturaFile(fileName string) bool {
	return strings.HasPrefix(fileName, futuraModuleBasePath)
}

func isExecuteReplayCallFlowFrame(frame runtime.Frame) bool {
	file, line := executeReplayCallFlowLocation()
	return frame.File == file && frame.Line == line
}
