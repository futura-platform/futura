package replay

import (
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
)

var futuraModuleBasePath string

func init() {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("failed to get caller")
	}
	// we need to get the base path of the futura package,
	// we can infer that at runtime by walking up the frame path
	// based on the known layout of this module. If the layout ever changes,
	// this will panic and let us know to update this code.
	thisPackageModulePath := []string{"internal", "flow", "replay", "futuraModuleBasePath.go"}
	slices.Reverse(thisPackageModulePath)
	for _, fileBase := range thisPackageModulePath {
		if fileBase != filepath.Base(file) {
			panic(fmt.Sprintf("expected file base %s, got %s", fileBase, filepath.Base(file)))
		}
		file = filepath.Dir(file)
	}
	futuraModuleBasePath = file
}
