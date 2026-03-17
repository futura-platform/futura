package golangci

import (
	"testing"

	"github.com/golangci/plugin-module-register/register"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/analysis"

	fanalysis "github.com/futura-platform/futura/analysis"
)

func TestPluginRegistration(t *testing.T) {
	newPlugin, err := register.GetPlugin("futura")
	require.NoError(t, err)

	plugin, err := newPlugin(nil)
	require.NoError(t, err)

	analyzers, err := plugin.BuildAnalyzers()
	require.NoError(t, err)
	require.Equal(t, analyzerNames(fanalysis.Suite()), analyzerNames(analyzers))

	expectedLoadMode := register.LoadModeSyntax
	if fanalysis.RequiresTypesInfo() {
		expectedLoadMode = register.LoadModeTypesInfo
	}

	require.Equal(t, expectedLoadMode, plugin.GetLoadMode())
}

func analyzerNames(analyzers []*analysis.Analyzer) []string {
	names := make([]string, 0, len(analyzers))
	for _, analyzer := range analyzers {
		names = append(names, analyzer.Name)
	}

	return names
}
