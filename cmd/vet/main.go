package main

import (
	fanalysis "github.com/futura-platform/futura/analysis"
	"golang.org/x/tools/go/analysis/unitchecker"
)

func main() {
	unitchecker.Main(fanalysis.Suite()...)
}
