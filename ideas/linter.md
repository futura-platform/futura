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

## A moment function must not close over values that vary per call

Identity is by source location: where the step is reached and where the fn is declared. Captured state
is not part of it, so two closures from one factory (`mk(1)`, `mk(2)`), or method values on two
receivers, are the same moment. Pass the varying value as the step's args, or key on it with `WithKey`;
never smuggle it through a capture.

```go
fn := mk(n)                         // no: n is captured, invisible to the runtime
futura.Step(b, fn, struct{}{})
futura.Step(b, run, n)              // yes: n is an arg, part of the memo
futura.Step(b.WithKey(id), fn, ...) // yes: id keys the moment
```

## Step inputs and state values should be comparable by `==`

The runtime compares a step's memoized input, and a state's current value, with `==` first. That is
the fast path on every replay. When `==` says "different", it falls back to comparing the encoded
bytes, so values that `==` cannot compare correctly (pointers, which are fresh on every replay; NaN,
which is never equal to itself) still hit their memo. The fallback is correct but costs an encode of
both values on every replay, so it is a performance concern, not a correctness one.

```go
futura.Step(b, fn, &cfg)          // works, but re-encodes cfg on every replay to prove it is unchanged
futura.Step(b, fn, cfg)           // yes: == answers on the fast path
futura.State(b, math.NaN())       // works via the fallback; avoid if the value can be represented otherwise
```

Flag pointer-typed and interface-typed step inputs and state types, and float fields that can carry NaN,
as "will always take the slow comparison". Do not treat them as errors.
