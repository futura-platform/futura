# Linter ideas

Rules that the runtime cannot enforce cheaply, or at all, but that a static check could.

## A moment function must only use the context it is given

A step's fn receives a context. Everything it does with a context must use that one, or one derived
from it. It must never reach for a context from an enclosing scope.

```go
futura.Step(b, func(ctx context.Context, _ struct{}) (int, error) {
    return futura.Step(b, ...)       // no: b is the enclosing flow's builder (and a nested step, see below)
    return futura.Step(b.WithContext(ctx), ...) // also no, for the nesting rule below
    return doWork(b, ...)            // no: b again
    return doWork(ctx, ...)          // yes
}, ...)
```

Why: the enclosing builder carries the replay's sequence state and goroutine binding. Using it from
inside a step lets the step act as if it were the flow, which is how a nested step would end up
recording at its parent's index. The runtime rejects a nested step through any context of the replay
(`step.ErrNestedStep`), but it cannot see a step handing the parent builder to a helper that does not
evaluate a step, and it cannot distinguish a step that uses the parent context for cancellation from
one that uses it for identity. The linter can: flag any reference to a `FlowBuilder` or flow
`context.Context` from an enclosing scope inside a function literal passed as a moment fn.

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

A method value taken on an interface variable is the same case: `g.Greet` is declared once, on the
interface, and the implementation behind it is captured state. Two implementations reached through
the same interface variable at one callsite are one moment. Key on the implementation, or pass it as
an arg. A method value on a concrete receiver is fine: each method is its own declaration.

```go
var g Greeter = pick()
futura.Source(b, g.Greet)                    // no: English and French are one moment
futura.Source(b.WithKey(g.Name()), g.Greet)  // yes
```

Low priority: a type argument is captured state too, but only when it is absent from the fn's
signature. `pick[int]` and `pick[string]` below have the same type, so one variable holds either, and
the runtime names both `pick[...]` at one declaration line. With `T` in a parameter or result the
instantiations are different types, and the assignment does not compile.

```go
func pick[T any](ctx context.Context) (string, error) { ... } // T only in the body
fn := pick[int]
fn = pick[string]                                   // compiles
futura.Source(b, fn)                                // no: both instantiations are one moment
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

An interface-typed value is a different matter: it satisfies `comparable`, but `==` on two interfaces
holding an uncomparable dynamic type (a slice, a map, a func, or a struct with one) panics at runtime
instead of reaching the fallback. The first execution is fine, since a memo miss compares nothing; the
panic lands on the next replay, when the memo is consulted. The fast path is not guarded, on purpose.
The linter can: flag a step input or State type that is an interface, or a struct with an interface
field, whose implementations are not all comparable.

```go
type input struct{ Id string; Handler }   // Handler is an interface
futura.Step(b, fn, input{..., sliceHandler{}}) // no: panics on the next replay
```

Flag pointer-typed and interface-typed step inputs and state types, and float fields that can carry NaN,
as "will always take the slow comparison". Do not treat them as errors.

## A struct that embeds a `BinaryMarshaler` must declare its own `MarshalBinary`

Embedding promotes the embedded type's `MarshalBinary` onto the outer struct, so the outer struct
satisfies `encoding.BinaryMarshaler` by accident. The encoder honours that, like `encoding/json` and
`encoding/gob` do, and the promoted method marshals the embedded value alone: every sibling field is
silently dropped.

```go
type Stamped struct {
    time.Time          // promotes MarshalBinary
    Extra int          // never encoded
}
```

Why the runtime cannot catch it: reflection and the runtime have no notion of "declared here" versus
"promoted". The compiler's wrapper flag (and the `<autogenerated>` file it reports) is set on the
promoted forwarder and on the pointer-receiver forwarder of a genuinely declared value method alike,
so it cannot tell the two apart. The type checker can. Flag any struct with an embedded field whose
type (or pointer type) implements `encoding.BinaryMarshaler`, unless the struct declares
`MarshalBinary` itself.

