package momentdiscovery

import (
	"reflect"

	"github.com/futura-platform/futura/analysis/concrete"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/buildssa"
	"golang.org/x/tools/go/callgraph/cha"
)

type MomentSite struct {
	Input  concrete.MonomorphicTypeInstantiationSite
	Output concrete.MonomorphicTypeInstantiationSite
}

type DiscoveryResult struct {
	CompleteSites []MomentSite
	InputOnly     []concrete.MonomorphicTypeInstantiationSite
	OutputOnly    []concrete.MonomorphicTypeInstantiationSite
}

var Analyzer = &analysis.Analyzer{
	Name:       "momentdiscover",
	Doc:        "discover package-local calls that may reach futura.Step. Returns a list of monomorphic moment types, with associated instantiation sites.",
	Requires:   []*analysis.Analyzer{buildssa.Analyzer},
	ResultType: reflect.TypeFor[DiscoveryResult](),
	Run:        run,
}

func run(pass *analysis.Pass) (any, error) {
	ssaRes := pass.ResultOf[buildssa.Analyzer].(*buildssa.SSA)
	cg := cha.CallGraph(ssaRes.Pkg.Prog)

	// 1. find step function calls
	stepFuncs := findFuturaStepFuncs(cg)
	var completeSites []MomentSite
	var inputOnly []concrete.MonomorphicTypeInstantiationSite
	var outputOnly []concrete.MonomorphicTypeInstantiationSite
	for _, stepFunc := range stepFuncs {
		// 2. Get a concrete type for the input and output
		inputTypeSites, err := concrete.ResolveConcreteTypePossibilities(cg, stepFunc, 0)
		if err != nil {
			return nil, err
		}
		outputTypeSites, err := concrete.ResolveConcreteTypePossibilities(cg, stepFunc, 1)
		if err != nil {
			return nil, err
		}

		complete, input, output := joinSites(inputTypeSites, outputTypeSites)
		completeSites = append(completeSites, complete...)
		inputOnly = append(inputOnly, input...)
		outputOnly = append(outputOnly, output...)
	}

	return DiscoveryResult{
		CompleteSites: completeSites,
		InputOnly:     inputOnly,
		OutputOnly:    outputOnly,
	}, nil
}
