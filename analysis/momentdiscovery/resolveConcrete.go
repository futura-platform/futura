package momentdiscovery

import (
	"go/types"

	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/ssa"
)

func resolveConcreteTyped(
	cg *callgraph.Graph,
	v ssa.Value,
	generic types.TypeParam,
) []ssa.Value {
	if isConcrete(v.Type()) {
		return []ssa.Value{v}
	}

	parent := v.Parent()
	if parent == nil {
		return nil
	}

	return nil
}

func isConcrete(t types.Type) bool {
	switch t := t.Underlying().(type) {
	case *types.Interface, *types.TypeParam:
		return false
	case *types.Struct:
		for field := range t.Fields() {
			if !isConcrete(field.Type()) {
				return false
			}
		}
		return true
	case *types.Signature:
		if params := t.Params(); params != nil {
			for v := range params.Variables() {
				if !isConcrete(v.Type()) {
					return false
				}
			}
		}
		if results := t.Results(); results != nil {
			for v := range results.Variables() {
				if !isConcrete(v.Type()) {
					return false
				}
			}
		}
		return true
	case *types.Array:
		return isConcrete(t.Elem())
	case *types.Slice:
		return isConcrete(t.Elem())
	case *types.Pointer:
		return isConcrete(t.Elem())
	case *types.Map:
		return isConcrete(t.Key()) && isConcrete(t.Elem())
	case *types.Chan:
		return isConcrete(t.Elem())
	default:
		return true
	}
}
