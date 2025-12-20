package moment

import (
	"fmt"
	"strings"
)

type Callsite struct {
	File string
	Line int
}

// Callpath is a list of callsites that form a path through the code.
// the 0th callsite is the outermost/highest call
// the nth callsite is the innermost/lowest call
type Callpath []Callsite

func (c Callpath) String() string {
	b := new(strings.Builder)
	for i, callsite := range c {
		fmt.Fprintf(b, "%s:%d", callsite.File, callsite.Line)
		if i < len(c)-1 {
			b.WriteString(" -> \n")
		}
	}

	return b.String()
}
