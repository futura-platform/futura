package moment

import (
	"runtime"
	"strings"
)

func CompileTimeLabel(fn *runtime.Func) string {
	fullName := fn.Name()
	lastPartStart := strings.LastIndex(fullName, "/")
	typeParamsStart := strings.LastIndex(fullName, "[")
	if typeParamsStart == -1 {
		typeParamsStart = len(fullName)
	}
	moduleAndFuncName := fullName[lastPartStart+1 : typeParamsStart]
	funcNameStart := strings.LastIndex(moduleAndFuncName, ".")
	return moduleAndFuncName[funcNameStart+1:]
}
