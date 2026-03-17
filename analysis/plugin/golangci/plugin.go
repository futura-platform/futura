package golangci

import (
	"github.com/golangci/plugin-module-register/register"
	"golang.org/x/tools/go/analysis"

	fanalysis "github.com/futura-platform/futura/analysis"
)

func init() {
	register.Plugin("futura", New)
}

// Settings holds golangci-lint plugin configuration.
type Settings struct{}

type Plugin struct {
	settings Settings
}

// New constructs the Futura golangci-lint module plugin.
func New(settings any) (register.LinterPlugin, error) {
	decoded, err := register.DecodeSettings[Settings](settings)
	if err != nil {
		return nil, err
	}

	return &Plugin{settings: decoded}, nil
}

// BuildAnalyzers returns the default Futura analyzer suite.
func (p *Plugin) BuildAnalyzers() ([]*analysis.Analyzer, error) {
	return fanalysis.Suite(), nil
}

// GetLoadMode reports the go/packages load mode required by the suite.
func (p *Plugin) GetLoadMode() string {
	if fanalysis.RequiresTypesInfo() {
		return register.LoadModeTypesInfo
	}

	return register.LoadModeSyntax
}
