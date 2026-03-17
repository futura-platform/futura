package validinput

import (
	"fmt"
	"go/types"

	"github.com/futura-platform/futura/analysis/concrete"
	"github.com/futura-platform/futura/analysis/momentdiscovery"
	"golang.org/x/tools/go/analysis"
)

var Analyzer = &analysis.Analyzer{
	Name:     "validinput",
	Doc:      "report Futura step inputs that do not follow rules or best practices",
	Requires: []*analysis.Analyzer{momentdiscovery.Analyzer},
	Run:      run,
}

func run(pass *analysis.Pass) (any, error) {
	discovery := pass.ResultOf[momentdiscovery.Analyzer].(momentdiscovery.DiscoveryResult)
	allInputSites := make([]concrete.MonomorphicTypeInstantiationSite, 0, len(discovery.InputOnly)+len(discovery.CompleteSites))
	allInputSites = append(allInputSites, discovery.InputOnly...)
	for _, site := range discovery.CompleteSites {
		allInputSites = append(allInputSites, site.Input)
	}

	for _, site := range allInputSites {
		if err := validateType(pass, site, site.Type, ""); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

const (
	inputValidationCategory = "validinput"
)

func validateType(pass *analysis.Pass, fromSite concrete.MonomorphicTypeInstantiationSite, t types.Type, accessExpression string) error {
	reportNotSerializable := func(t types.Type) {
		pass.Report(analysis.Diagnostic{
			Pos:      fromSite.Pos,
			Category: inputValidationCategory,
			Message:  fmt.Sprintf("input type contains a type that cannot be serialized: %s.", t),
			SuggestedFixes: []analysis.SuggestedFix{{
				Message: fmt.Sprintf("Use a serializable type instead of a %s.", t),
			}},
		})
	}
	switch t := types.Unalias(t).(type) {
	case *types.Named:
		return validateType(pass, fromSite, t.Underlying(), accessExpression)
	case *types.Basic:
		switch t.Kind() {
		case types.Uintptr, types.UnsafePointer:
			reportNotSerializable(t)
		}
	case *types.Interface:
		// Generic constraints can surface as interfaces while traversing a
		// resolved type parameter. We don't validate interfaces yet.
		return nil
	case *types.TypeParam:
		subSites, ok := fromSite.SubGenerics[t]
		if !ok {
			panic(fmt.Errorf("unexpected type param discovered @ %s", accessExpression))
		}
		for _, subSite := range subSites {
			if err := validateType(pass, subSite, subSite.Type, accessExpression+"<"+t.String()+">"); err != nil {
				return err
			}
		}
	case *types.Struct:
		for field := range t.Fields() {
			if err := validateType(pass, fromSite, field.Type(), accessExpression+"."+field.Name()); err != nil {
				return err
			}
		}
	case *types.Slice:
		return validateType(pass, fromSite, t.Elem(), accessExpression+"[]")
	case *types.Array:
		return validateType(pass, fromSite, t.Elem(), accessExpression+"[index]")
	case *types.Map:
		if err := validateType(pass, fromSite, t.Key(), accessExpression+"[key]"); err != nil {
			return err
		}
		if err := validateType(pass, fromSite, t.Elem(), accessExpression+"[value]"); err != nil {
			return err
		}
	case *types.Pointer:
		message := "input type contains a pointer"
		if accessExpression != "" {
			message = "input type contains a pointer at " + accessExpression
		}

		pass.Report(analysis.Diagnostic{
			Pos:      fromSite.Pos,
			Category: inputValidationCategory,
			Message:  message,
			SuggestedFixes: []analysis.SuggestedFix{{
				Message: "Use a value type instead of a pointer.",
			}},
			Related: []analysis.RelatedInformation{
				{
					// Pos:     reportPosition,
					Message: "Pointers in inputs are discouraged because they cannot be serialized reliably and may cause unexpected moment invalidation.",
				},
			},
		})
		return validateType(pass, fromSite, t.Elem(), accessExpression)
	case *types.Signature, *types.Chan:
		reportNotSerializable(t)
	default:
		return fmt.Errorf("unexpected type: %s", t)
	}
	return nil
}
