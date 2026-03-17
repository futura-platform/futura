package validinput

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/checker"
	"golang.org/x/tools/go/packages"
)

func TestAnalyzer(t *testing.T) {
	t.Run("direct step call", func(t *testing.T) {
		assertPointerDiagnosticCount(
			t,
			"github.com/futura-platform/futura/analysis/validinput/testdata/directinputpointerstub",
			1,
		)
	})

	t.Run("generic step wrapper call", func(t *testing.T) {
		assertPointerDiagnosticCount(
			t,
			"github.com/futura-platform/futura/analysis/validinput/testdata/genericinputpointerstub",
			1,
		)
	})

	t.Run("combined direct and generic calls", func(t *testing.T) {
		assertPointerDiagnosticCount(
			t,
			"github.com/futura-platform/futura/analysis/validinput/testdata/noinputpointersstub",
			2,
		)
	})
}

type diagnosticInfo struct {
	filename string
	line     int
	message  string
}

func assertPointerDiagnosticCount(t *testing.T, pattern string, wantCount int) {
	t.Helper()

	diags := analyzePackage(t, pattern)
	var pointerDiagnostics []string
	for _, diag := range diags {
		if strings.Contains(diag.message, "input type contains a pointer") {
			pointerDiagnostics = append(pointerDiagnostics, formatDiagnostic(diag))
		}
	}

	require.Lenf(t, pointerDiagnostics, wantCount,
		"pointer diagnostics: %v, all diagnostics: %v",
		pointerDiagnostics, formatDiagnostics(diags))
}

func analyzePackage(t *testing.T, pattern string) []diagnosticInfo {
	t.Helper()

	root := moduleRoot(t)
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedImports |
			packages.NeedTypes | packages.NeedTypesSizes | packages.NeedSyntax | packages.NeedTypesInfo |
			packages.NeedDeps | packages.NeedModule,
		Dir:   root,
		Tests: false,
		Env:   append(os.Environ(), "GO111MODULE=on", "GOPROXY=off", "GOWORK=off"),
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		cfg.Env = append(os.Environ(), "GOPATH="+root, "GO111MODULE=off", "GOWORK=off")
	}
	pkgs, err := packages.Load(cfg, pattern)
	require.NoError(t, err, "load packages")
	require.NotEmpty(t, pkgs, "failed to load test package")
	require.NotEmpty(t, pkgs[0].Name, "failed to load test package: %v", pkgs)

	graph, err := checker.Analyze([]*analysis.Analyzer{Analyzer}, pkgs, nil)
	require.NoError(t, err, "analyze")

	var diags []diagnosticInfo
	for _, act := range graph.Roots {
		require.NoError(t, act.Err, "analysis error")
		for _, d := range act.Diagnostics {
			pos := act.Package.Fset.Position(d.Pos)
			diags = append(diags, diagnosticInfo{
				filename: pos.Filename,
				line:     pos.Line,
				message:  d.Message,
			})
		}
	}

	return diags
}

func formatDiagnostics(diags []diagnosticInfo) []string {
	formatted := make([]string, len(diags))
	for i, diag := range diags {
		formatted[i] = formatDiagnostic(diag)
	}
	return formatted
}

func formatDiagnostic(diag diagnosticInfo) string {
	return filepath.Base(diag.filename) + ":" + fmt.Sprintf("%d", diag.line) + ": " + diag.message
}

func moduleRoot(t *testing.T) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok, "unable to determine module root")
	// From analysis/validinput/analyzer_test.go -> two levels up to module root
	return filepath.Join(filepath.Dir(filename), "..", "..")
}
