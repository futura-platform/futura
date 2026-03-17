package momentdiscovery

import (
	mapset "github.com/deckarep/golang-set/v2"
	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/ssa"
)

func findFuturaStepFuncs(cg *callgraph.Graph) []*ssa.Function {
	var out []*ssa.Function
	seen := mapset.NewThreadUnsafeSet[*ssa.Function]()
	for fn := range cg.Nodes {
		if fn == nil {
			continue
		}
		canonical := fn
		if origin := fn.Origin(); origin != nil {
			canonical = origin
		}
		if !isFuturaStepFunc(canonical) {
			continue
		}
		if seen.Contains(canonical) {
			continue
		}
		seen.Add(canonical)
		out = append(out, canonical)
	}
	return out
}

func isFuturaStepFunc(fn *ssa.Function) bool {
	if fn == nil {
		return false
	}

	if origin := fn.Origin(); origin != nil {
		fn = origin
	}

	obj := fn.Object()
	if obj == nil {
		return false
	}
	pkg := obj.Pkg()
	if pkg == nil {
		return false
	}

	return pkg.Path() == "github.com/futura-platform/futura" && obj.Name() == "Step"
}
