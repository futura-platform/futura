package fanalysis

import (
	"golang.org/x/tools/go/analysis"

	"github.com/futura-platform/futura/analysis/validinput"
)

// Suite returns the default set of Futura analyzers.
func Suite() []*analysis.Analyzer {
	return []*analysis.Analyzer{
		validinput.Analyzer,
	}
}

// RequiresTypesInfo reports whether the default analyzer suite needs type info.
func RequiresTypesInfo() bool {
	return true
}
