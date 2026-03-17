package momentdiscovery

import (
	"fmt"
	"go/types"
	"strings"
	"testing"

	"github.com/futura-platform/futura/analysis/concrete"
	"github.com/futura-platform/futura/internal/utils/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/ssa"
)

type comparableType struct {
	marker string
}

func newComparableType(marker string) types.Type {
	return &comparableType{marker: marker}
}

// String implements [types.Type].
func (c *comparableType) String() string {
	return c.marker
}

// Underlying implements [types.Type].
func (c *comparableType) Underlying() types.Type {
	panic("unimplemented")
}

// siteToCompactString renders a MonomorphicTypeInstantiationSite for test assertions.
// It uses %p for ssa.Function instead of fn.String(), avoiding massive verbosity
// when ElementsMatch fails.
func siteToCompactString(s concrete.MonomorphicTypeInstantiationSite) string {
	callpath := make([]string, len(s.Callpath))
	for i, fn := range s.Callpath {
		callpath[i] = fmt.Sprintf("%p", fn)
	}
	return fmt.Sprintf("%s|%d|%s", s.Type.String(), s.Pos, strings.Join(callpath, " -> "))
}

func requireSitesMatch(t *testing.T, expected, actual []concrete.MonomorphicTypeInstantiationSite) {
	t.Helper()
	expectedStrs := make([]string, len(expected))
	for i, s := range expected {
		expectedStrs[i] = siteToCompactString(s)
	}
	actualStrs := make([]string, len(actual))
	for i, s := range actual {
		actualStrs[i] = siteToCompactString(s)
	}
	require.ElementsMatch(t, expectedStrs, actualStrs)
}

func TestJoinSites(t *testing.T) {
	t.Run("emits all possible combonations of input and output sites", func(t *testing.T) {
		fn1 := new(ssa.Function)
		fn2 := new(ssa.Function)
		fn3 := new(ssa.Function)
		t.Run("an input and output on the same callpath", func(t *testing.T) {
			inputSite1 := concrete.MonomorphicTypeInstantiationSite{
				Type:     newComparableType("input"),
				Callpath: []*ssa.Function{fn1, fn2, fn3},
			}
			outputSite1 := concrete.MonomorphicTypeInstantiationSite{
				Type:     newComparableType("output"),
				Callpath: []*ssa.Function{fn1, fn2, fn3},
			}
			complete, input, output := joinSites(
				[]concrete.MonomorphicTypeInstantiationSite{inputSite1},
				[]concrete.MonomorphicTypeInstantiationSite{outputSite1},
			)
			require.ElementsMatch(t, []MomentSite{{
				Input:  inputSite1,
				Output: outputSite1,
			}}, complete)
			require.Empty(t, input)
			require.Empty(t, output)
		})
		t.Run("multiple downstream outputs for a single input", func(t *testing.T) {
			inputSite := concrete.MonomorphicTypeInstantiationSite{
				Type:     newComparableType("input1"),
				Callpath: []*ssa.Function{fn1},
			}
			outputSite1 := concrete.MonomorphicTypeInstantiationSite{
				Type:     newComparableType("output1"),
				Callpath: []*ssa.Function{fn1, fn2},
			}
			outputSite2 := concrete.MonomorphicTypeInstantiationSite{
				Type:     newComparableType("output2"),
				Callpath: []*ssa.Function{fn1, fn3},
			}
			complete, input, output := joinSites(
				[]concrete.MonomorphicTypeInstantiationSite{inputSite},
				[]concrete.MonomorphicTypeInstantiationSite{outputSite1, outputSite2},
			)
			require.ElementsMatch(t, []MomentSite{{
				Input:  inputSite,
				Output: outputSite1,
			}, {
				Input:  inputSite,
				Output: outputSite2,
			}}, complete)
			require.Empty(t, input)
			require.Empty(t, output)
		})
		t.Run("multiple upstream inputs for a single output", func(t *testing.T) {
			outputSite := concrete.MonomorphicTypeInstantiationSite{
				Type:     newComparableType("output"),
				Callpath: []*ssa.Function{fn1},
			}
			inputSite1 := concrete.MonomorphicTypeInstantiationSite{
				Type:     newComparableType("input1"),
				Callpath: []*ssa.Function{fn1, fn2},
			}
			inputSite2 := concrete.MonomorphicTypeInstantiationSite{
				Type:     newComparableType("input2"),
				Callpath: []*ssa.Function{fn1, fn3},
			}
			complete, input, output := joinSites(
				[]concrete.MonomorphicTypeInstantiationSite{inputSite1, inputSite2},
				[]concrete.MonomorphicTypeInstantiationSite{outputSite},
			)
			require.ElementsMatch(t, []MomentSite{{
				Input:  inputSite1,
				Output: outputSite,
			}, {
				Input:  inputSite2,
				Output: outputSite,
			}}, complete)
			require.Empty(t, input)
			require.Empty(t, output)
		})
		t.Run("an input and output on different callpaths", func(t *testing.T) {
			inputSite := concrete.MonomorphicTypeInstantiationSite{
				Type:     newComparableType("input"),
				Callpath: []*ssa.Function{fn1, fn2},
			}
			outputSite := concrete.MonomorphicTypeInstantiationSite{
				Type:     newComparableType("output"),
				Callpath: []*ssa.Function{fn1, fn3},
			}
			complete, input, output := joinSites(
				[]concrete.MonomorphicTypeInstantiationSite{inputSite},
				[]concrete.MonomorphicTypeInstantiationSite{outputSite},
			)
			require.Empty(t, complete)
			requireSitesMatch(t, []concrete.MonomorphicTypeInstantiationSite{inputSite}, input)
			requireSitesMatch(t, []concrete.MonomorphicTypeInstantiationSite{outputSite}, output)
		})
		t.Run("invariant cases", func(t *testing.T) {
			t.Run("multiple inputs along the same callpath", func(t *testing.T) {
				t.Run("exact", func(t *testing.T) {
					inputSite1 := concrete.MonomorphicTypeInstantiationSite{
						Type:     newComparableType("input1"),
						Callpath: []*ssa.Function{fn1, fn2},
					}
					inputSite2 := concrete.MonomorphicTypeInstantiationSite{
						Type:     newComparableType("input2"),
						Callpath: []*ssa.Function{fn1, fn2},
					}
					testutil.PanicsWithErrorIs(t, ErrOverlappingMatches, func() {
						joinSites(
							[]concrete.MonomorphicTypeInstantiationSite{inputSite1, inputSite2},
							[]concrete.MonomorphicTypeInstantiationSite{},
						)
					})
				})
				t.Run("indirect", func(t *testing.T) {
					inputSite1 := concrete.MonomorphicTypeInstantiationSite{
						Type:     newComparableType("input1"),
						Callpath: []*ssa.Function{fn1, fn2},
					}
					inputSite2 := concrete.MonomorphicTypeInstantiationSite{
						Type:     newComparableType("input2"),
						Callpath: []*ssa.Function{fn1, fn2, fn3},
					}
					testutil.PanicsWithErrorIs(t, ErrOverlappingMatches, func() {
						joinSites(
							[]concrete.MonomorphicTypeInstantiationSite{inputSite1, inputSite2},
							[]concrete.MonomorphicTypeInstantiationSite{},
						)
					})
				})
			})
			t.Run("multiple outputs along the same callpath", func(t *testing.T) {
				t.Run("exact", func(t *testing.T) {
					outputSite1 := concrete.MonomorphicTypeInstantiationSite{
						Type:     newComparableType("output1"),
						Callpath: []*ssa.Function{fn1, fn2},
					}
					outputSite2 := concrete.MonomorphicTypeInstantiationSite{
						Type:     newComparableType("output2"),
						Callpath: []*ssa.Function{fn1, fn2},
					}
					testutil.PanicsWithErrorIs(t, ErrOverlappingMatches, func() {
						joinSites(
							[]concrete.MonomorphicTypeInstantiationSite{},
							[]concrete.MonomorphicTypeInstantiationSite{outputSite1, outputSite2},
						)
					})
				})
				t.Run("indirect", func(t *testing.T) {
					outputSite1 := concrete.MonomorphicTypeInstantiationSite{
						Type:     newComparableType("output1"),
						Callpath: []*ssa.Function{fn1, fn2},
					}
					outputSite2 := concrete.MonomorphicTypeInstantiationSite{
						Type:     newComparableType("output2"),
						Callpath: []*ssa.Function{fn1, fn2, fn3},
					}
					testutil.PanicsWithErrorIs(t, ErrOverlappingMatches, func() {
						joinSites(
							[]concrete.MonomorphicTypeInstantiationSite{},
							[]concrete.MonomorphicTypeInstantiationSite{outputSite1, outputSite2},
						)
					})
				})
			})
		})
	})
}
