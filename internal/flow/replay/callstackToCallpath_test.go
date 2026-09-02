package replay

import (
	"runtime"
	"testing"

	"github.com/futura-platform/futura/moment"
	"github.com/stretchr/testify/assert"
)

func TestCallstackToCallpath(t *testing.T) {
	site := func(function, file string, line int) moment.Callsite {
		return CallstackToCallpath([]runtime.Frame{{Function: function, File: file, Line: line}})[0]
	}

	t.Run("a callsite is the package's import path, the file's name, and the line", func(t *testing.T) {
		assert.Equal(t,
			moment.Callsite{File: "github.com/acme/app/flows/checkout.go", Line: 42},
			site("github.com/acme/app/flows.Checkout", "/home/ci/src/app/flows/checkout.go", 42),
		)
	})
	t.Run("the build location does not affect the callsite", func(t *testing.T) {
		a := site("github.com/acme/app/flows.Checkout", "/home/alice/app/flows/checkout.go", 42)
		b := site("github.com/acme/app/flows.Checkout", "/var/build/1234/app/flows/checkout.go", 42)
		assert.Equal(t, a, b)
	})
	t.Run("a dependency's version does not affect the callsite", func(t *testing.T) {
		a := site("github.com/futura-platform/futura.Step[...]", "/go/pkg/mod/github.com/futura-platform/futura@v0.0.28/step.go", 19)
		b := site("github.com/futura-platform/futura.Step[...]", "/go/pkg/mod/github.com/futura-platform/futura@v0.0.29/step.go", 19)
		assert.Equal(t, a, b)
		assert.Equal(t, "github.com/futura-platform/futura/step.go", a.File)
	})
	t.Run("a changed line still changes the callsite", func(t *testing.T) {
		a := site("github.com/acme/app.Run", "/x/app/run.go", 5)
		b := site("github.com/acme/app.Run", "/x/app/run.go", 6)
		assert.NotEqual(t, a, b)
	})
	t.Run("methods, closures, and generic functions resolve to their package", func(t *testing.T) {
		for _, function := range []string{
			"github.com/acme/app/flows.Checkout",
			"github.com/acme/app/flows.(*Cart).Total",
			"github.com/acme/app/flows.Checkout.func1",
			"github.com/acme/app/flows.Retry[...]",
			"github.com/acme/app/flows.Checkout.Retry[...].func2",
		} {
			assert.Equal(t, "github.com/acme/app/flows/f.go", site(function, "/x/f.go", 1).File, function)
		}
	})
	t.Run("a versioned last segment belongs to the package, not the declaration", func(t *testing.T) {
		assert.Equal(t, "gopkg.in/yaml.v3/decode.go", site("gopkg.in/yaml.v3.(*Decoder).Decode", "/x/decode.go", 1).File)
		assert.Equal(t, "gopkg.in/yaml.v3/decode.go", site("gopkg.in/yaml.v3.Unmarshal", "/x/decode.go", 1).File)
		assert.Equal(t, "gopkg.in/inf.v0/dec.go", site("gopkg.in/inf.v0.(*Dec).Add", "/x/dec.go", 1).File)
		assert.Equal(t, "gopkg.in/check.v1/check.go", site("gopkg.in/check.v1.(*C).Assert", "/x/check.go", 1).File)
		assert.Equal(t, "main/main.go", site("main.main", "/x/main.go", 1).File)
		// a declaration that merely starts with v followed by digits is still a declaration
		assert.Equal(t, "github.com/acme/app/f.go", site("github.com/acme/app.v2Handler", "/x/f.go", 1).File)
	})
}
