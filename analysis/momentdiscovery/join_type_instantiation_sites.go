package momentdiscovery

import (
	"errors"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/futura/analysis/concrete"
	"github.com/samber/mo"
	"golang.org/x/tools/go/ssa"
)

type inputIndex int
type outputIndex int
type callpathTrie struct {
	next map[*ssa.Function]*callpathTrie

	inputTerminalIndex  mo.Option[inputIndex]
	outputTerminalIndex mo.Option[outputIndex]
}

var ErrOverlappingMatches = errors.New("multiple matches found on the same path for the same generic type")

// joinSites joins input and output sites that happen down/upstream of one another.
// For any inputs or output instantiation sites that did not have any accompanying output or input sites,
// they are returned in the inputOnly or outputOnly slices.
func joinSites(
	inputSites,
	outputSites []concrete.MonomorphicTypeInstantiationSite,
) (
	completeSites []MomentSite,
	inputOnly,
	outputOnly []concrete.MonomorphicTypeInstantiationSite,
) {
	root := &callpathTrie{
		next: make(map[*ssa.Function]*callpathTrie),
	}
	// build the trie
	for i, site := range inputSites {
		currentNode := root
		for _, fn := range site.Callpath {
			if nextNode, ok := currentNode.next[fn]; ok {
				currentNode = nextNode
			} else {
				newNext := &callpathTrie{next: make(map[*ssa.Function]*callpathTrie)}
				currentNode.next[fn] = newNext
				currentNode = newNext
			}
		}
		if currentNode.inputTerminalIndex.IsSome() {
			panic(ErrOverlappingMatches)
		}
		currentNode.inputTerminalIndex = mo.Some(inputIndex(i))
	}
	for i, site := range outputSites {
		currentNode := root
		for _, fn := range site.Callpath {
			if nextNode, ok := currentNode.next[fn]; ok {
				currentNode = nextNode
			} else {
				newNext := &callpathTrie{next: make(map[*ssa.Function]*callpathTrie)}
				currentNode.next[fn] = newNext
				currentNode = newNext
			}
		}
		if currentNode.outputTerminalIndex.IsSome() {
			panic(ErrOverlappingMatches)
		}
		currentNode.outputTerminalIndex = mo.Some(outputIndex(i))
	}

	momentSites := make([]MomentSite, 0, len(inputSites)+len(outputSites))
	unmatchedInputs := mapset.NewThreadUnsafeSet[inputIndex]()
	unmatchedOutputs := mapset.NewThreadUnsafeSet[outputIndex]()
	for i := range inputSites {
		unmatchedInputs.Add(inputIndex(i))
	}
	for i := range outputSites {
		unmatchedOutputs.Add(outputIndex(i))
	}
	// traverse DFS (recusively for readability) to match inputs with outputs
	var matchDfs func(node *callpathTrie, downStreamMatch mo.Option[mo.Either[inputIndex, outputIndex]])
	matchDfs = func(node *callpathTrie, downStreamMatch mo.Option[mo.Either[inputIndex, outputIndex]]) {
		switch {
		case node.inputTerminalIndex.IsSome() && node.outputTerminalIndex.IsSome():
			if downStreamMatch.IsSome() {
				panic(ErrOverlappingMatches)
			}
			inputIndex := node.inputTerminalIndex.MustGet()
			outputIndex := node.outputTerminalIndex.MustGet()
			momentSites = append(momentSites, MomentSite{
				Input:  inputSites[inputIndex],
				Output: outputSites[outputIndex],
			})
			unmatchedInputs.Remove(inputIndex)
			unmatchedOutputs.Remove(outputIndex)
			return

		case node.inputTerminalIndex.IsSome():
			existingMatch, ok := downStreamMatch.Get()
			if ok && existingMatch.IsLeft() {
				panic(ErrOverlappingMatches)
			}
			inIndex := node.inputTerminalIndex.MustGet()
			if ok {
				outIndex := existingMatch.MustRight()
				momentSites = append(momentSites, MomentSite{
					Output: outputSites[outIndex],
					Input:  inputSites[inIndex],
				})
				unmatchedOutputs.Remove(outIndex)
				unmatchedInputs.Remove(inIndex)
			}
			downStreamMatch = mo.Some(mo.Left[inputIndex, outputIndex](inIndex))
		case node.outputTerminalIndex.IsSome():
			existingMatch, ok := downStreamMatch.Get()
			if ok && existingMatch.IsRight() {
				panic(ErrOverlappingMatches)
			}
			outIndex := node.outputTerminalIndex.MustGet()
			if ok {
				inIndex := existingMatch.MustLeft()
				momentSites = append(momentSites, MomentSite{
					Output: outputSites[outIndex],
					Input:  inputSites[inIndex],
				})
				unmatchedInputs.Remove(inIndex)
				unmatchedOutputs.Remove(outIndex)
			}
			downStreamMatch = mo.Some(mo.Right[inputIndex](outIndex))
		}

		for _, nextNode := range node.next {
			matchDfs(nextNode, downStreamMatch)
		}
	}
	matchDfs(root, mo.None[mo.Either[inputIndex, outputIndex]]())

	outputOnly = make([]concrete.MonomorphicTypeInstantiationSite, 0, unmatchedOutputs.Cardinality())
	for _, outIndex := range unmatchedOutputs.ToSlice() {
		outputOnly = append(outputOnly, outputSites[outIndex])
	}
	inputOnly = make([]concrete.MonomorphicTypeInstantiationSite, 0, unmatchedInputs.Cardinality())
	for _, inIndex := range unmatchedInputs.ToSlice() {
		inputOnly = append(inputOnly, inputSites[inIndex])
	}

	return momentSites, inputOnly, outputOnly
}
