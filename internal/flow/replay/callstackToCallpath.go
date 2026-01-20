package replay

import (
	"runtime"

	"github.com/futura-platform/futura/moment"
)

func CallstackToCallpath(callstack []runtime.Frame) moment.Callpath {
	p := make(moment.Callpath, len(callstack))
	for i, frame := range callstack {
		p[i] = moment.Callsite{File: frame.File, Line: frame.Line}
	}
	return p
}
