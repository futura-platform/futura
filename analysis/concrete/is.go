package concrete

import (
	"go/types"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/samber/mo"
)

type genericSubType mo.Either[*types.Interface, *types.TypeParam]

func getSubGenerics(t types.Type) (subTypeParams mapset.Set[*types.TypeParam], subInterfaces mapset.Set[*types.Interface]) {
	subTypeParams = mapset.NewThreadUnsafeSet[*types.TypeParam]()
	subInterfaces = mapset.NewThreadUnsafeSet[*types.Interface]()
	accumulateRecurse := func(rt types.Type) {
		subSubGenerics, subSubInterfaces := getSubGenerics(rt)
		subTypeParams = subTypeParams.Union(subSubGenerics)
		subInterfaces = subInterfaces.Union(subSubInterfaces)
	}
	switch t := t.(type) {
	case *types.Named:
		for arg := range t.TypeArgs().Types() {
			accumulateRecurse(arg)
		}
		// ignore the sub type params here since we've already accumulated them from the type args
		_, subSubInterfaces := getSubGenerics(t.Underlying())
		subInterfaces = subInterfaces.Union(subSubInterfaces)
	case *types.Alias:
		for arg := range t.TypeArgs().Types() {
			accumulateRecurse(arg)
		}
		// ignore the sub type params here since we've already accumulated them from the type args
		_, subSubInterfaces := getSubGenerics(t.Rhs())
		subInterfaces = subInterfaces.Union(subSubInterfaces)
	case *types.Interface:
		subInterfaces.Add(t)
	case *types.TypeParam:
		subTypeParams.Add(t)
	case *types.Struct:
		for field := range t.Fields() {
			accumulateRecurse(field.Type())
		}
	case *types.Signature:
		if params := t.Params(); params != nil {
			for v := range params.Variables() {
				accumulateRecurse(v.Type())
			}
		}
		if results := t.Results(); results != nil {
			for v := range results.Variables() {
				accumulateRecurse(v.Type())
			}
		}
	case *types.Array:
		accumulateRecurse(t.Elem())
	case *types.Slice:
		accumulateRecurse(t.Elem())
	case *types.Pointer:
		accumulateRecurse(t.Elem())
	case *types.Map:
		accumulateRecurse(t.Key())
		accumulateRecurse(t.Elem())
	case *types.Chan:
		accumulateRecurse(t.Elem())
	}

	return subTypeParams, subInterfaces
}
