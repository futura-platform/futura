# Linter ideas

Rules that the runtime cannot enforce cheaply, or at all, but that a static check could.

## A moment function must only use the context it is given

A step's fn receives a context. Everything it does with a context must use that one, or one derived
from it. It must never reach for a context from an enclosing scope.

```go
futura.Step(b, func(ctx context.Context, _ struct{}) (int, error) {
    return futura.Step(b, ...)       // no: b is the enclosing flow's builder
    return futura.Step(b.WithContext(ctx), ...) // also no, but for the nesting rule below
    return doWork(b, ...)            // no: b again
    return doWork(ctx, ...)          // yes
}, ...)
```

Why: the enclosing builder carries the replay's sequence state and goroutine binding. Using it from
inside a step lets the step act as if it were the flow, which is how a nested step ends up recording
at its parent's index. The runtime now rejects the nested-step case specifically (`step.ErrNestedStep`),
but it cannot see a step handing the parent builder to a helper that does not evaluate a step, and it
cannot distinguish a step that uses the parent context for cancellation from one that uses it for
identity. The linter can: flag any reference to a `FlowBuilder` or flow `context.Context` from an
enclosing scope inside a function literal passed as a moment fn.

## Steps are leaves

A moment fn must not call `Step`, `Effect`, `Source`, `Action`, `State`, or `When`. Enforced at runtime
by `step.ErrNestedStep`, but a linter catches it before the flow runs.

## Purity outside steps

The flow function is pure except inside steps. A linter can flag I/O, time, randomness, and goroutine
creation outside a moment fn. This is the rule the runtime relies on most and enforces least.

## Steps run in the flow body, never in a defer

A step must not be evaluated from a Go `defer` in the flow function, nor from a `futura.Defer` callback.

```go
func flowFn(b futura.FlowBuilder, _ struct{}) (int, error) {
    defer func() { futura.Action(b, cleanup) }()   // no
    futura.Defer(b, func() { futura.Action(b, cleanup) }) // no: runs after the replay, panics
    ...
}
```

Why: a deferred closure's parent frame reports the line of whichever `return` is unwinding, so the
same step gets a different identity depending on the return path. `futura.Defer` runs after the replay
has ended, so a step inside it has no replay to record into. The runtime cannot tell a deferred call
from a direct one (the frames are identical), so this is a linter rule: flag `defer` statements in a
flow function whose closure references the builder, and flag step calls inside a `futura.Defer` callback.
