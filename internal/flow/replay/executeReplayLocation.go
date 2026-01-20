package replay

import (
	"embed"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"runtime"
	"sync"

	"github.com/futura-platform/futura/moment"
)

const (
	sourceFileName = "execute.go"
)

//go:embed execute.go
var source embed.FS

var executeReplayDiscovery sync.Once

type fileAndLine struct {
	file string
	line int
}

var executeReplayLocation fileAndLine

//go:noinline
func executeReplayCallFlowLocation() (file string, line int) {
	executeReplayDiscovery.Do(func() {
		// discover the line
		fs := token.NewFileSet()
		executeReplaySrc, err := source.Open(sourceFileName)
		if err != nil {
			panic(err)
		}
		f, err := parser.ParseFile(fs, sourceFileName, executeReplaySrc, parser.AllErrors)
		if err != nil {
			panic(err)
		}

		executeValue := reflect.ValueOf(Execute[any, any])
		fnPtr := executeValue.Pointer()
		fn := runtime.FuncForPC(fnPtr)

		// discover the file
		executeReplayLocation.file, _ = fn.FileLine(fnPtr)

		// derive the execute function name from the reflect type
		executeFunctionName := moment.CompileTimeLabel(fn)

		// derive the callable flow param name form the reflect type
		possibleCallableFlowParamIndices := []int{}
		for i := range executeValue.Type().NumIn() {
			paramType := executeValue.Type().In(i)
			if paramType.Kind() == reflect.Func {
				possibleCallableFlowParamIndices = append(possibleCallableFlowParamIndices, i)
			}
		}
		if len(possibleCallableFlowParamIndices) != 1 {
			panic(fmt.Sprintf("expected exactly one callable flow param, found: %v", possibleCallableFlowParamIndices))
		}
		callableFlowParamIndex := possibleCallableFlowParamIndices[0]

		// find the function, assuming it is defined top level
		var executeDecl *ast.FuncDecl
		for _, stmt := range f.Decls {
			if funcDecl, ok := stmt.(*ast.FuncDecl); ok && funcDecl.Name.Name == executeFunctionName {
				executeDecl = funcDecl
				break
			}
		}
		if executeDecl == nil {
			panic("executeReplay function not found")
		}

		// now that we have the execute function ast, we can find the callable flow param name
		var callableFlowParam *ast.Ident
		for i, l := range executeDecl.Type.Params.List {
			for j, ident := range l.Names {
				paramIndex := i + j
				if paramIndex == callableFlowParamIndex {
					callableFlowParam = ident
					break
				}
			}
		}
		if callableFlowParam == nil {
			panic(fmt.Sprintf("callable flow param not found: %d", callableFlowParamIndex))
		}

		// now finally we can find the callsite of the callable flow param
		possibleCalls := []*ast.CallExpr{}
		ast.Inspect(executeDecl, func(node ast.Node) bool {
			if callExpr, ok := node.(*ast.CallExpr); ok {
				if ident, ok := callExpr.Fun.(*ast.Ident); ok && ident.Name == callableFlowParam.Name {
					possibleCalls = append(possibleCalls, callExpr)
					return false
				}
			}
			return true
		})
		if len(possibleCalls) != 1 {
			panic(fmt.Sprintf("expected exactly one call to the callable flow param, got: %d", len(possibleCalls)))
		}
		callableFlowParamCall := possibleCalls[0]
		executeReplayLocation.line = fs.Position(callableFlowParamCall.Pos()).Line
	})

	return executeReplayLocation.file, executeReplayLocation.line
}
