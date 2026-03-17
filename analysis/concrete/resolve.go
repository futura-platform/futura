package concrete

import (
	"fmt"
	"go/token"
	"go/types"
	"slices"
	"strings"

	mapset "github.com/deckarep/golang-set/v2"
	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/ssa"
)

// MonomorphicTypeInstantiationSite is a site where a type is instantiated.
// All sub generics are gauranteed to have their own MonomorphicTypeInstantiationSite attached (except in a cycle).
type MonomorphicTypeInstantiationSite struct {
	Type types.Type
	Pos  token.Pos
	// Callpath is ordered from the function being resolved outward through callers.
	// The 0th element is the generic function whose type parameter is being resolved,
	// and the last element is the caller where the concrete type was found.
	Callpath []*ssa.Function

	SubGenerics map[*types.TypeParam][]MonomorphicTypeInstantiationSite
	// TODO: make subinterfaces also a map of possibilities
	SubInterfaces mapset.Set[*types.Interface]
}

type resolutionState struct {
	Function *ssa.Function
	Index    int
}

// ResolveConcreteTypePossibilities resolves a generic type parameter
// into a monomorphic list of possible types, based on the known call graph.
func ResolveConcreteTypePossibilities(
	cg *callgraph.Graph,
	fn *ssa.Function,
	resolveTypeParamIndex int,
) ([]MonomorphicTypeInstantiationSite, error) {
	return resolveConcreteTypePossibilities(cg, fn, resolveTypeParamIndex, []*ssa.Function{fn}, map[resolutionState]struct{}{})
}

func resolveConcreteTypePossibilities(
	cg *callgraph.Graph,
	fn *ssa.Function,
	resolveTypeParamIndex int,
	callpath []*ssa.Function,
	active map[resolutionState]struct{},
) ([]MonomorphicTypeInstantiationSite, error) {
	state := resolutionState{Function: fn, Index: resolveTypeParamIndex}
	if _, seen := active[state]; seen {
		return nil, nil
	}
	active[state] = struct{}{}
	defer delete(active, state)

	var possibilities []MonomorphicTypeInstantiationSite
	for _, call := range incomingCalls(cg, fn) {
		sites, err := resolveConcreteTypePossibilitiesForCall(cg, call, resolveTypeParamIndex, callpath, active)
		if err != nil {
			return nil, err
		}
		possibilities = append(possibilities, sites...)
	}
	return dedupeSites(possibilities), nil
}

// incomingCalls includes edges for instantiated wrapper functions too, since
// generic call sites may target a wrapper whose Origin() is fn instead of fn itself.
func incomingCalls(cg *callgraph.Graph, fn *ssa.Function) []*callgraph.Edge {
	seen := mapset.NewThreadUnsafeSet[*callgraph.Edge]()
	var in []*callgraph.Edge

	addNodeIn := func(node *callgraph.Node) {
		if node == nil {
			return
		}
		for _, edge := range node.In {
			if edge == nil {
				continue
			}
			if seen.Contains(edge) {
				continue
			}
			seen.Add(edge)
			in = append(in, edge)
		}
	}

	addNodeIn(cg.Nodes[fn])
	for candidate, node := range cg.Nodes {
		if candidate == nil || candidate == fn {
			continue
		}
		if candidate.Origin() == fn {
			addNodeIn(node)
		}
	}

	return in
}

func resolveConcreteTypePossibilitiesForCall(
	cg *callgraph.Graph,
	call *callgraph.Edge,
	resolveTypeParamIndex int,
	callpath []*ssa.Function,
	active map[resolutionState]struct{},
) ([]MonomorphicTypeInstantiationSite, error) {
	if isSyntheticWrapperCall(call) {
		// Collapse SSA forwarding wrappers so callpaths reflect source-level
		// callers while still preserving type information from the wrapper.
		return resolveConcreteTypePossibilities(cg, call.Caller.Func, resolveTypeParamIndex, callpath, active)
	}

	callpath = append(slices.Clone(callpath), call.Caller.Func)
	targetTypeArg, err := resolveTypeArgFromCall(call, resolveTypeParamIndex)
	if err != nil {
		return nil, err
	}
	subTypeParams, subInterfaces := getSubGenerics(targetTypeArg)
	instantiationSite := MonomorphicTypeInstantiationSite{
		Type:          targetTypeArg,
		Pos:           resolveInstantiationPos(call),
		Callpath:      callpath,
		SubGenerics:   make(map[*types.TypeParam][]MonomorphicTypeInstantiationSite, subTypeParams.Cardinality()),
		SubInterfaces: subInterfaces,
	}

	for subTypeParam := range subTypeParams.Iter() {
		// search for the concrete type of this sub-generic type parameter in the caller
		subPossibilities, err := resolveConcreteTypePossibilities(
			cg,
			call.Caller.Func,
			subTypeParam.Index(),
			[]*ssa.Function{call.Caller.Func},
			active,
		)
		if err != nil {
			return nil, err
		}
		instantiationSite.SubGenerics[subTypeParam] = subPossibilities
	}

	return []MonomorphicTypeInstantiationSite{instantiationSite}, nil
}

func isSyntheticWrapperCall(call *callgraph.Edge) bool {
	if call.Caller.Func.Origin() == call.Callee.Func {
		return true
	}

	synthetic := call.Caller.Func.Synthetic
	if strings.HasPrefix(synthetic, "instantiation wrapper of ") {
		return true
	}

	// Interface method dispatch may introduce an additional SSA method wrapper
	// between the invoke site and the instantiation wrapper.
	return strings.HasPrefix(synthetic, "wrapper for func ")
}

// resolveTypeArgFromCall resolves a type argument from a call graph edge.
// The SSA builder will sometimes put the type arguments in the callee function when it has a generic receiver,
// but not always. This function handles all cases.
func resolveTypeArgFromCall(call *callgraph.Edge, resolveTypeParamIndex int) (types.Type, error) {
	calleeTypeArgs := call.Callee.Func.TypeArgs()
	if len(calleeTypeArgs) > resolveTypeParamIndex {
		return calleeTypeArgs[resolveTypeParamIndex], nil
	}
	if call.Callee.Func.Signature.Recv() == nil {
		return nil, fmt.Errorf(
			"expected call %q -> %q to expose type arg %d",
			call.Caller.Func.Name(),
			call.Callee.Func.Name(),
			resolveTypeParamIndex,
		)
	}
	return resolveReceiverTypeArgFromCall(call, resolveTypeParamIndex)
}

func resolveReceiverTypeArgFromCall(call *callgraph.Edge, resolveTypeParamIndex int) (types.Type, error) {
	common := call.Site.Common()
	var recvType types.Type
	if common.IsInvoke() {
		recvType = common.Value.Type()
	} else if len(common.Args) > 0 {
		recvType = common.Args[0].Type()
	} else {
		return nil, fmt.Errorf(
			"expected receiver-bearing call %q -> %q to expose receiver type arg %d",
			call.Caller.Func.Name(),
			call.Callee.Func.Name(),
			resolveTypeParamIndex,
		)
	}
	return receiverTypeArgAtIndex(recvType, resolveTypeParamIndex)
}

func receiverTypeArgAtIndex(recvType types.Type, resolveTypeParamIndex int) (types.Type, error) {
	for {
		switch current := types.Unalias(recvType).(type) {
		case *types.Pointer:
			recvType = current.Elem()
			continue
		case *types.Named:
			typeArgs := current.TypeArgs()
			if typeArgs.Len() <= resolveTypeParamIndex {
				return nil, fmt.Errorf("receiver type %q has no type arg %d", recvType.String(), resolveTypeParamIndex)
			}
			return typeArgs.At(resolveTypeParamIndex), nil
		default:
			return nil, fmt.Errorf("receiver type %q does not expose named type args", recvType.String())
		}
	}
}

func resolveInstantiationPos(call *callgraph.Edge) token.Pos {
	if pos := call.Site.Pos(); pos.IsValid() {
		return pos
	}
	return call.Caller.Func.Pos()
}

func dedupeSites(sites []MonomorphicTypeInstantiationSite) []MonomorphicTypeInstantiationSite {
	seen := make(map[string]struct{}, len(sites))
	deduped := make([]MonomorphicTypeInstantiationSite, 0, len(sites))
	for _, site := range sites {
		key := site.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		deduped = append(deduped, site)
	}
	return deduped
}

func (site MonomorphicTypeInstantiationSite) String() string {
	callpath := make([]string, len(site.Callpath))
	for i, fn := range site.Callpath {
		callpath[i] = fn.String()
	}
	return fmt.Sprintf("%s|%d|%s", site.Type.String(), site.Pos, strings.Join(callpath, " -> "))
}
