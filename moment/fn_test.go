package moment

import (
	"context"
	"testing"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/utils/testutil"
	"github.com/stretchr/testify/assert"
)

func TestFnLabel(t *testing.T) {
	t.Run("compile time label", func(t *testing.T) {
		label := NewFn(labelToInfer).Label()
		compileTimeLabel := CompileTimeLabel(runtimeFunc(labelToInfer))
		assert.Equal(t, compileTimeLabel, label)

		label = NewFn(func(ctx context.Context, args *any) (any, error) { return nil, nil }).Label()
		assert.Equal(t, "1", label)
	})

	t.Run("runtime label", func(t *testing.T) {
		test1Labelled := NewFn(labelToInfer, ftype.WithLabel("test1"))
		test2Labelled := NewFn(labelToInfer, ftype.WithLabel("test2"))

		assert.Equal(t, "test1", test1Labelled.Label())
		assert.Equal(t, "test2", test2Labelled.Label())
	})
}

func TestFnOptions(t *testing.T) {
	t.Run("applies from first to last", func(t *testing.T) {
		label1 := "label1"
		label2 := "label2"
		opts := []ftype.MomentFnOption{ftype.WithLabel(label1), ftype.WithLabel(label2)}
		fn := NewFn(labelToInfer, opts...)
		assert.Equal(t, label2, fn.Label())
		assert.Equal(t, opts, fn.Options())
	})
}

func TestFnCall(t *testing.T) {
	identity := NewIdentity(context.Background(), Callpath{{File: "a.go", Line: 1}})
	t.Run("invokes the callable with the args", func(t *testing.T) {
		fn := NewFn(func(ctx context.Context, args string) (string, error) { return args, nil })
		r, err := fn.Call(context.Background(), identity, "test")
		assert.NoError(t, err)
		assert.Equal(t, "test", r)
	})
	t.Run("the callable can read its own identity, and only while it executes", func(t *testing.T) {
		fn := NewFn(func(ctx context.Context, _ string) (Identity, error) { return CurrentIdentity(ctx), nil })
		seen, err := fn.Call(context.Background(), identity, "")
		assert.NoError(t, err)
		assert.Equal(t, identity, seen)

		testutil.PanicsWithErrorIs(t, ErrNoMomentBeingEvaluated, func() {
			CurrentIdentity(context.Background())
		})
	})
}
