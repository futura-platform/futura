package concrete

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/analysis/analysistest"
	"golang.org/x/tools/go/analysis/passes/buildssa"
	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/callgraph/cha"
	"golang.org/x/tools/go/ssa"
	"k8s.io/utils/diff"
)

// CallGetter extracts an *callgraph.Edge from a parsed SSA program/package for resolution.
type CallGetter func(cg *callgraph.Graph, prog *ssa.Program, pkg *ssa.Package) *ssa.Function

type comparableSite struct {
	Type            string
	Callpath        []string
	SubExpectations map[string][]comparableSite
}

// toComparableSites returns the sites in a comparable form, and in a normalized order.
func toComparableSites(sites []MonomorphicTypeInstantiationSite) []comparableSite {
	comparableUnordered := make([]comparableSite, 0, len(sites))
	for _, site := range sites {
		cSite := comparableSite{
			Type:     site.Type.String(),
			Callpath: callpathNames(site.Callpath),
		}

		if len(site.SubGenerics) > 0 {
			cSite.SubExpectations = make(map[string][]comparableSite, len(site.SubGenerics))

			for k, v := range site.SubGenerics {
				cSite.SubExpectations[k.String()] = toComparableSites(v)
			}
		}

		comparableUnordered = append(comparableUnordered, cSite)
	}

	// sort by type and callpath for consistent ordering
	sortComparableSites(comparableUnordered)
	return comparableUnordered
}

func sortComparableSites(sites []comparableSite) []comparableSite {
	sort.Slice(sites, func(i, j int) bool {
		if sites[i].Type != sites[j].Type {
			return sites[i].Type < sites[j].Type
		}
		return strings.Join(sites[i].Callpath, "") < strings.Join(sites[j].Callpath, "")
	})
	return sites
}

// comparableSitesEqual asserts expected and got are equal after sorting both.
// On failure, prints a diff via diff.ObjectGoPrintSideBySide.
func comparableSitesEqual(t *testing.T, expected, got []comparableSite) bool {
	t.Helper()
	expSorted := sortComparableSites(append([]comparableSite{}, expected...))
	gotSorted := sortComparableSites(append([]comparableSite{}, got...))
	return assert.Equal(t, expSorted, gotSorted, diff.ObjectGoPrintSideBySide(expSorted, gotSorted))
}

// runResolveTest loads src as a Go package, builds SSA and callgraph, uses getValue
// to obtain a value, runs ResolveConcreteTypes on it, and returns the result.
func runResolveTest(t *testing.T, src []byte, getValue CallGetter) []comparableSite {
	return runResolveTestAtIndex(t, src, getValue, 0)
}

func runResolveTestAtIndex(t *testing.T, src []byte, getValue CallGetter, resolveTypeParamIndex int) []comparableSite {
	t.Helper()
	tmp := t.TempDir()
	pkgDir := filepath.Join(tmp, "src", "testpackage")
	if err := os.MkdirAll(pkgDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pkgDir, "parser_cases.go"), src, 0o644); err != nil {
		t.Fatal(err)
	}

	result := analysistest.Run(t, tmp, buildssa.Analyzer, "testpackage")[0].Result
	ssaRes := result.(*buildssa.SSA)
	cg := cha.CallGraph(ssaRes.Pkg.Prog)

	fn := getValue(cg, ssaRes.Pkg.Prog, ssaRes.Pkg)
	sites, err := ResolveConcreteTypePossibilities(cg, fn, resolveTypeParamIndex)
	require.NoError(t, err)
	return toComparableSites(sites)
}

func getFunctionToResolve(name string) CallGetter {
	return func(cg *callgraph.Graph, prog *ssa.Program, pkg *ssa.Package) *ssa.Function {
		for _, m := range pkg.Members {
			fn, ok := m.(*ssa.Function)
			if !ok {
				continue
			}
			origin := fn.Origin()
			if origin == nil {
				origin = fn
			}
			if origin.Name() == name {
				inEdges := cg.Nodes[fn].In
				if len(inEdges) == 0 {
					panic("no in edges")
				}
				return fn
			}
		}
		return nil
	}
}

func inlineSource(src string) []byte {
	return []byte(strings.TrimLeft(src, "\n"))
}

func callpathNames(callpath []*ssa.Function) []string {
	names := make([]string, len(callpath))
	for i, fn := range callpath {
		names[i] = fn.Name()
	}
	return names
}

func TestResolveConcreteTypes_singleTypeParam(t *testing.T) {
	t.Run("inferred caller", func(t *testing.T) {
		got := runResolveTest(t, inlineSource(`
package testpackage

type inferredCallerType struct{}

func InferredCaller() {
	callee(inferredCallerType{})
}

func callee[T any](toResolve T) {}
`), getFunctionToResolve("callee"))
		comparableSitesEqual(t, []comparableSite{
			{
				Type:     "testpackage.inferredCallerType",
				Callpath: []string{"callee", "InferredCaller"},
			},
		}, got)
	})

	t.Run("duplicate instantiation sites keep separate callers", func(t *testing.T) {
		got := runResolveTest(t, inlineSource(`
package testpackage

type inferredCallerType struct{}

func InferredCaller() {
	callee(inferredCallerType{})
}

func DuplicateInferredCaller() {
	callee(inferredCallerType{})
}

func callee[T any](toResolve T) {}
`), getFunctionToResolve("callee"))
		comparableSitesEqual(t, []comparableSite{
			{
				Type:     "testpackage.inferredCallerType",
				Callpath: []string{"callee", "InferredCaller"},
			},
			{
				Type:     "testpackage.inferredCallerType",
				Callpath: []string{"callee", "DuplicateInferredCaller"},
			},
		}, got)
	})

	t.Run("explicit type argument", func(t *testing.T) {
		got := runResolveTest(t, inlineSource(`
package testpackage

type explicitCallerType struct{}

func ExplicitCaller() {
	callee[explicitCallerType](explicitCallerType{})
}

func callee[T any](toResolve T) {}
`), getFunctionToResolve("callee"))
		comparableSitesEqual(t, []comparableSite{
			{
				Type:     "testpackage.explicitCallerType",
				Callpath: []string{"callee", "ExplicitCaller"},
			},
		}, got)
	})

	t.Run("indirect generic caller", func(t *testing.T) {
		got := runResolveTest(t, inlineSource(`
package testpackage

func genericCaller[U any](toResolve U) {
	callee(toResolve)
}

type indirectCallerType struct{}

func IndirectFuncCaller() {
	genericCaller(indirectCallerType{})
}

func callee[T any](toResolve T) {}
`), getFunctionToResolve("callee"))
		comparableSitesEqual(t, []comparableSite{
			{
				Type:     "U",
				Callpath: []string{"callee", "genericCaller"},
				SubExpectations: map[string][]comparableSite{
					"U": {
						{
							Type:     "testpackage.indirectCallerType",
							Callpath: []string{"genericCaller", "IndirectFuncCaller"},
						},
					},
				},
			},
		}, got)
	})

	t.Run("multi-hop generic caller", func(t *testing.T) {
		got := runResolveTest(t, inlineSource(`
package testpackage

func genericCaller[U1 any](toResolve U1) {
	callee(toResolve)
}

func middleGenericCaller[U2 any](toResolve U2) {
	genericCaller(toResolve)
}

func outerGenericCaller[U3 any](toResolve U3) {
	middleGenericCaller(toResolve)
}

type multiHopCallerType struct{}

func MultiHopCaller() {
	outerGenericCaller(multiHopCallerType{})
}

func callee[T any](toResolve T) {}
`), getFunctionToResolve("callee"))
		comparableSitesEqual(t, []comparableSite{
			{
				Type: "U1",
				Callpath: []string{
					"callee",
					"genericCaller",
				},
				SubExpectations: map[string][]comparableSite{
					"U1": {
						{
							Type:     "U2",
							Callpath: []string{"genericCaller", "middleGenericCaller"},
							SubExpectations: map[string][]comparableSite{
								"U2": {
									{
										Type:     "U3",
										Callpath: []string{"middleGenericCaller", "outerGenericCaller"},
										SubExpectations: map[string][]comparableSite{
											"U3": {
												{
													Type:     "testpackage.multiHopCallerType",
													Callpath: []string{"outerGenericCaller", "MultiHopCaller"},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}, got)
	})

	t.Run("cyclic caller", func(t *testing.T) {
		got := runResolveTest(t, inlineSource(`
package testpackage

func cycleA[T any](toResolve T) {
	callee(toResolve)
	cycleB(toResolve)
}

func cycleB[U any](toResolve U) {
	cycleA(toResolve)
}

type cyclicCallerType struct{}

func CyclicCaller() {
	cycleA(cyclicCallerType{})
}

func callee[T any](toResolve T) {}
`), getFunctionToResolve("callee"))
		comparableSitesEqual(t, []comparableSite{
			{
				Type:     "T",
				Callpath: []string{"callee", "cycleA"},
				SubExpectations: map[string][]comparableSite{
					"T": {
						{
							Type:     "U",
							Callpath: []string{"cycleA", "cycleB"},
							SubExpectations: map[string][]comparableSite{
								"U": {
									{
										Type:     "T",
										Callpath: []string{"cycleB", "cycleA"},
										SubExpectations: map[string][]comparableSite{
											// should stop here since it's a cycle
											"T": {},
										},
									},
								},
							},
						},
						{
							Type:     "testpackage.cyclicCallerType",
							Callpath: []string{"cycleA", "CyclicCaller"},
						},
					},
				},
			},
		}, got)
	})

	t.Run("generic method receiver", func(t *testing.T) {
		got := runResolveTest(t, inlineSource(`
package testpackage

type genericReceiver[T any] struct{}

func (genericReceiver[T]) genericMethod() {
	var toResolve T
	callee(toResolve)
}

type indirectMethodCallerType struct{}

func IndirectMethodCaller() {
	genericReceiver[indirectMethodCallerType]{}.genericMethod()
}

func callee[T any](toResolve T) {}
`), getFunctionToResolve("callee"))
		comparableSitesEqual(t, []comparableSite{
			{
				Type:     "T",
				Callpath: []string{"callee", "genericMethod"},
				SubExpectations: map[string][]comparableSite{
					"T": {
						{
							Type:     "testpackage.indirectMethodCallerType",
							Callpath: []string{"genericMethod", "IndirectMethodCaller"},
						},
					},
				},
			},
		}, got)
	})

	t.Run("interface dispatch", func(t *testing.T) {
		got := runResolveTest(t, inlineSource(`
package testpackage

type genericReceiver[T any] struct{}

func (genericReceiver[T]) genericMethod() {
	var toResolve T
	callee(toResolve)
}

type genericInterfaceReceiver[T any] interface {
	genericMethod()
}

type indirectInterfaceCallerType struct{}

func NewGenericInterface() genericInterfaceReceiver[indirectInterfaceCallerType] {
	return genericReceiver[indirectInterfaceCallerType]{}
}

func IndirectInterfaceCaller() {
	NewGenericInterface().genericMethod()
}

func callee[T any](toResolve T) {}
`), getFunctionToResolve("callee"))
		comparableSitesEqual(t, []comparableSite{
			{
				Type:     "T",
				Callpath: []string{"callee", "genericMethod"},
				SubExpectations: map[string][]comparableSite{
					"T": {
						{
							Type:     "testpackage.indirectInterfaceCallerType",
							Callpath: []string{"genericMethod", "IndirectInterfaceCaller"},
						},
					},
				},
			},
		}, got)
	})

	t.Run("pointer receiver method", func(t *testing.T) {
		got := runResolveTest(t, inlineSource(`
package testpackage

type genericPointerReceiver[T any] struct{}

func (*genericPointerReceiver[T]) genericMethod() {
	var toResolve T
	callee(toResolve)
}

type indirectPointerMethodCallerType struct{}

func IndirectPointerMethodCaller() {
	(&genericPointerReceiver[indirectPointerMethodCallerType]{}).genericMethod()
}

func callee[T any](toResolve T) {}
`), getFunctionToResolve("callee"))
		comparableSitesEqual(t, []comparableSite{
			{
				Type:     "T",
				Callpath: []string{"callee", "genericMethod"},
				SubExpectations: map[string][]comparableSite{
					"T": {
						{
							Type:     "testpackage.indirectPointerMethodCallerType",
							Callpath: []string{"genericMethod", "IndirectPointerMethodCaller"},
						},
					},
				},
			},
		}, got)
	})

	t.Run("mapped generic caller", func(t *testing.T) {
		got := runResolveTest(t, inlineSource(`
package testpackage

type indirectMappedGenericCallerType[T any] struct {
	field T
}

func indirectGenericMapperCaller[T any]() {
	callee(indirectMappedGenericCallerType[T]{})
}

func IndirectMappedGenericCaller() {
	indirectGenericMapperCaller[int]()
}

func callee[T any](toResolve T) {}
`), getFunctionToResolve("callee"))
		comparableSitesEqual(t, []comparableSite{
			{
				Type:     "testpackage.indirectMappedGenericCallerType[T]",
				Callpath: []string{"callee", "indirectGenericMapperCaller"},
				SubExpectations: map[string][]comparableSite{
					"T": {
						{
							Type:     "int",
							Callpath: []string{"indirectGenericMapperCaller", "IndirectMappedGenericCaller"},
						},
					},
				},
			},
		}, got)
	})

	t.Run("array type", func(t *testing.T) {
		got := runResolveTest(t, inlineSource(`
package testpackage

type arrayCallerElem struct{}

func ArrayCaller() {
	callee([1]arrayCallerElem{})
}

func callee[T any](toResolve T) {}
`), getFunctionToResolve("callee"))
		comparableSitesEqual(t, []comparableSite{
			{
				Type:     "[1]testpackage.arrayCallerElem",
				Callpath: []string{"callee", "ArrayCaller"},
			},
		}, got)
	})

	t.Run("slice type", func(t *testing.T) {
		got := runResolveTest(t, inlineSource(`
package testpackage

type sliceCallerElem struct{}

func SliceCaller() {
	callee([]sliceCallerElem{})
}

func callee[T any](toResolve T) {}
`), getFunctionToResolve("callee"))
		comparableSitesEqual(t, []comparableSite{
			{
				Type:     "[]testpackage.sliceCallerElem",
				Callpath: []string{"callee", "SliceCaller"},
			},
		}, got)
	})

	t.Run("pointer type", func(t *testing.T) {
		got := runResolveTest(t, inlineSource(`
package testpackage

type pointerCallerElem struct{}

func PointerCaller() {
	callee(&pointerCallerElem{})
}

func callee[T any](toResolve T) {}
`), getFunctionToResolve("callee"))
		comparableSitesEqual(t, []comparableSite{
			{
				Type:     "*testpackage.pointerCallerElem",
				Callpath: []string{"callee", "PointerCaller"},
			},
		}, got)
	})

	t.Run("map type", func(t *testing.T) {
		got := runResolveTest(t, inlineSource(`
package testpackage

type mapCallerValue struct{}

func MapCaller() {
	callee(map[string]mapCallerValue{})
}

func callee[T any](toResolve T) {}
`), getFunctionToResolve("callee"))
		comparableSitesEqual(t, []comparableSite{
			{
				Type:     "map[string]testpackage.mapCallerValue",
				Callpath: []string{"callee", "MapCaller"},
			},
		}, got)
	})

	t.Run("channel type", func(t *testing.T) {
		got := runResolveTest(t, inlineSource(`
package testpackage

type chanCallerElem struct{}

func ChanCaller() {
	callee(make(chan chanCallerElem))
}

func callee[T any](toResolve T) {}
`), getFunctionToResolve("callee"))
		comparableSitesEqual(t, []comparableSite{
			{
				Type:     "chan testpackage.chanCallerElem",
				Callpath: []string{"callee", "ChanCaller"},
			},
		}, got)
	})

	t.Run("struct type", func(t *testing.T) {
		got := runResolveTest(t, inlineSource(`
package testpackage

type structFieldType struct{}

type structCallerType struct {
	Field structFieldType
}

func StructCaller() {
	callee(structCallerType{})
}

func callee[T any](toResolve T) {}
`), getFunctionToResolve("callee"))
		comparableSitesEqual(t, []comparableSite{
			{
				Type:     "testpackage.structCallerType",
				Callpath: []string{"callee", "StructCaller"},
			},
		}, got)
	})

	t.Run("function signature type", func(t *testing.T) {
		got := runResolveTest(t, inlineSource(`
package testpackage

type signatureParam struct{}
type signatureResult struct{}

func SignatureCaller() {
	callee(func(signatureParam) signatureResult {
		return signatureResult{}
	})
}

func callee[T any](toResolve T) {}
`), getFunctionToResolve("callee"))
		comparableSitesEqual(t, []comparableSite{
			{
				Type:     "func(testpackage.signatureParam) testpackage.signatureResult",
				Callpath: []string{"callee", "SignatureCaller"},
			},
		}, got)
	})
}

func TestResolveConcreteTypes_secondTypeParam(t *testing.T) {
	t.Run("inferred second type argument", func(t *testing.T) {
		got := runResolveTestAtIndex(t, inlineSource(`
package testpackage

func calleeSecond[A, B any](a A, b B) {}

type secondParamInferredCallerType struct{}

func SecondParamInferredCaller() {
	calleeSecond(0, secondParamInferredCallerType{})
}
`), getFunctionToResolve("calleeSecond"), 1)
		comparableSitesEqual(t, []comparableSite{
			{
				Type:     "testpackage.secondParamInferredCallerType",
				Callpath: []string{"calleeSecond", "SecondParamInferredCaller"},
			},
		}, got)
	})

	t.Run("explicit second type argument", func(t *testing.T) {
		got := runResolveTestAtIndex(t, inlineSource(`
package testpackage

func calleeSecond[A, B any](a A, b B) {}

type secondParamExplicitCallerType struct{}

func SecondParamExplicitCaller() {
	calleeSecond[int, secondParamExplicitCallerType](0, secondParamExplicitCallerType{})
}
`), getFunctionToResolve("calleeSecond"), 1)
		comparableSitesEqual(t, []comparableSite{
			{
				Type:     "testpackage.secondParamExplicitCallerType",
				Callpath: []string{"calleeSecond", "SecondParamExplicitCaller"},
			},
		}, got)
	})

	t.Run("indirect second type argument", func(t *testing.T) {
		got := runResolveTestAtIndex(t, inlineSource(`
package testpackage

func calleeSecond[A, B any](a A, b B) {}

func genericSecondCaller[U any](toResolve U) {
	calleeSecond(0, toResolve)
}

type secondParamIndirectCallerType struct{}

func SecondParamIndirectCaller() {
	genericSecondCaller(secondParamIndirectCallerType{})
}
`), getFunctionToResolve("calleeSecond"), 1)
		comparableSitesEqual(t, []comparableSite{
			{
				Type:     "U",
				Callpath: []string{"calleeSecond", "genericSecondCaller"},
				SubExpectations: map[string][]comparableSite{
					"U": {
						{
							Type:     "testpackage.secondParamIndirectCallerType",
							Callpath: []string{"genericSecondCaller", "SecondParamIndirectCaller"},
						},
					},
				},
			},
		}, got)
	})
}
