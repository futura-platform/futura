package moment

import (
	"runtime"
	"strings"
)

func CompileTimeLabel(fn *runtime.Func) string {
	fullName := fn.Name()
	if end := strings.LastIndex(fullName, "}."); end != -1 {
		fullName = fullName[end+2:]
	}
	lastPartStart := strings.LastIndex(fullName, "/")
	typeParamsStart := strings.LastIndex(fullName, "[")
	if typeParamsStart <= lastPartStart {
		// no type parameters: the bracket, if any, is part of a type spelled out earlier in the name
		typeParamsStart = len(fullName)
	}
	// Strip the module path and type parameters, then keep only the final function identifier.
	moduleAndFuncName := fullName[lastPartStart+1 : typeParamsStart]
	funcNameStart := strings.LastIndex(moduleAndFuncName, ".")
	return moduleAndFuncName[funcNameStart+1:]
}
