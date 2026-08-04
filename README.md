# statemachine

Finite state machines in Go. Zero dependencies — standard library only, tests included.

```go
import "github.com/open-ships/statemachine"
```

📖 **[Reference documentation on pkg.go.dev](https://pkg.go.dev/github.com/open-ships/statemachine)**

## The idea

A finite state machine is a partial function from `(state, event)` to `state`. This package keeps
that function as its core and builds optional state owners around it.

**A `Machine` does not hold the current state.** It is an immutable compiled definition. State can
remain a value you own — a struct field, a database column — and a step is the application of the
definition to that value:

```go
order.State, err = orders.Fire(ctx, order.State, Pay, cmd)
```

One definition can serve a million aggregates and any number of goroutines. When the package should
own execution state instead, construct one `Instance` per aggregate. The [`queued`](queued) package
adds serialized run-to-completion execution, [`persist`](persist) runs the flat definition inside an
adapter-owned unit of work, and [`statechart`](statechart) supplies hierarchy and lifecycle actions.
A transactional `persist.Store` can pass its transaction through to effects. These modules remain
separate because they have different concurrency and failure semantics.

## Hello world

```go
type State string
type Event string

const (
	Off State = "off"
	On  State = "on"
)
const Flip Event = "flip"

var light = statemachine.MustCompile([]statemachine.Transition[State, Event, struct{}]{
	{From: Off, Event: Flip, To: On},
	{From: On, Event: Flip, To: Off},
})

func main() {
	s := Off
	s, _ = light.Fire(context.Background(), s, Flip, struct{}{})
	fmt.Println(s) // on
}
```

States and events are your own defined types with named constants, so `Flipp` and `Of` are compile
errors rather than a 3 a.m. page.

## A real machine

```go
// Note the =: an alias, not a defined type, so Compile can infer S, E and T.
type row = statemachine.Transition[State, Event, *Cmd]

var table = []row{
	{From: Draft,   Event: Submit, To: Pending,     Guard: hasLines},
	{From: Draft,   Event: Cancel, To: Cancelled},

	{From: Pending, Event: Pay,    To: Paid,        Do: charge},

	// Two rows share Paid+Ship. The first whose guard applies wins; the second
	// is the unguarded default arm.
	{From: Paid,    Event: Ship,   To: Shipped,     Guard: inStock},
	{From: Paid,    Event: Ship,   To: Backordered},
}

var orders = statemachine.MustCompile(table)
```

**Guards return a reason, not a bool.** `nil` applies, any error declines. Declining is not a
failure — it drops that row and tries the next one, so a trailing unguarded row is a default arm, the
semantics of a `switch`. Only when no row is left does the reason reach the caller:

```go
_, err := orders.Fire(ctx, Delivered, Refund, cmd)

errors.Is(err, statemachine.ErrNotPermitted) // true: the machine refused    -> 409
errors.Is(err, ErrWindowClosed)              // true: and this is why        -> 422
```

That is the whole 409-versus-422 story, with no second error type and no `errors.As`.

**Effects fail with `return err`.** `Fire` reports the destination if and only if `Do` returned `nil`,
and the error comes back to you unwrapped, so `errors.Is` against your own sentinels works with no
ceremony:

```go
{From: Pending, Event: Pay, To: Paid, Do: func(ctx context.Context, c *Cmd) error {
	return c.gateway.Charge(ctx, c.Order.ID, c.Order.Cents) // fails -> stays Pending
}},
```

**Affordances come from the same rule as firing**, so a rendered button and the handler that receives
its click cannot disagree about where an event leads:

```go
for event, to := range orders.Permitted(ctx, o.State, cmd) {
	fmt.Println(event, "->", to) // ship -> shipped, cancel -> refunded
}
```

## Interface

| | |
|---|---|
| `Transition[S, E comparable, T any]` | one row: `From`, `Event`, `To`, `Guard`, `Do` |
| `Machine[S, E comparable, T any]` | a compiled table; immutable, safe for concurrent use |
| `Compile(transitions)` | build a machine, reporting an unreachable row |
| `MustCompile(transitions)` | the same, panicking — for tables that are program text |
| `Machine.Fire(ctx, from, event, data)` | apply an event; report the state to move to |
| `Machine.Permitted(ctx, from, data)` | iterate the events accepted now, each with its destination |
| `ErrNotPermitted` | the sentinel every refusal wraps |

The optional state-owning interface adds:

| | |
|---|---|
| `NewInstance(machine, initial)` | create one fail-fast, in-memory execution |
| `NewInstanceWithObservers(machine, initial, observers...)` | create an execution that reports committed node exits and entries |
| `Instance.State()` | read its last committed state |
| `Instance.Fire(ctx, event, data)` | apply one event without passing the state |
| `Instance.Permitted(ctx, data)` | eagerly snapshot its current affordances |
| `ErrInFlight` | another fire is already executing on that instance |

An `Observation` names one exited or entered state. Its `Seq` is consecutive
within one observed execution, `Step` groups the non-empty position change made
by one event, and `Remaining == 0` closes that Step. Context and `T` are passed
to the typed `Observer` separately. Observers run synchronously without an
execution lock, but in isolation: an observer panic or `runtime.Goexit` cannot
change the transition outcome. Flat self-transitions change no position and are
observation-silent. A shared observer can be called concurrently by different
executions and must synchronize its own census or sink.

`T` is the value handed to every `Guard` and `Do` — your aggregate, plus whatever this command needs.
It is passed to `Fire` rather than stored, so one immutable `Machine` serves every request while
still seeing request-scoped values. A machine with nothing to carry uses `struct{}`.

Visualization and reachability checking remain ordinary loops over the flat table. Per-state entry
and exit actions do not belong to a flat `Machine`; use the `statechart` package when those semantics
are required. Runnable flat-machine examples are in [`example_test.go`](example_test.go).

## Choosing an execution model

| Need | Module | State and concurrency semantics |
|---|---|---|
| Database aggregate or explicit assignment | `Machine` | caller-owned |
| One in-memory aggregate | `Instance` | owned state; overlapping fire fails fast |
| Follow-up events and FIFO serialization | `queued.Runtime` | owned state; each root run drains to completion |
| Database state and outbox work | `persist.Fire` | transactional when the Store supplies a transaction; never auto-retried |
| Hierarchy, initial substates, entry/exit, reentry | `statechart` | immutable chart plus one stateful instance per aggregate |

A queued callback schedules same-runtime follow-ups with `queued.Enqueue` and the context it was
given. It must never synchronously call `Runtime.Fire`; replacing that context defeats deadlock
detection.

Statechart inspection is split between immutable definition facts and one
execution's current position. `Chart.States` enumerates compiled states,
`Chart.Arrows(source)` exposes every statically possible inherited transition
without running Guards, and `Chart.Position(active)` projects an exact loaded
state. `Instance.Position` takes the same atomic committed-state snapshot as
`State`. A Position classifies every declared state as inactive, enclosing, or
the exact active state. The original `Definition` remains the source for
declared parent and initial edges; the Chart API does not duplicate those
relationships.

## Hazards

**Discarding `Machine.Fire`'s returned state is never correct.** The effect has already run, and
neither the compiler nor `go vet` reports the lost transition. A state-owning `Instance.Fire` may
discard its returned state, but its error still must be handled.

An `Instance` or queued runtime is the sole owner of its state. Do not also keep an authoritative
copy in the value passed as `T`. Effects can still be partial: the state owner guarantees its state
transition, not rollback of arbitrary I/O. Its synchronization protects the owned state, not other
mutable fields inside `T`.

For persistence, a getter and setter are not a transaction. A `persist.Store` must use a conditional
write and must invoke the transition callback exactly once after a successful load. A transactional
database Store should put aggregate changes and an outbox record in the same unit of work. Only work
performed through that transaction is atomic. A conflict discovered after an effect is not retried;
an application retry must reload and use a stable idempotency key.
`persist.Step` additionally reports the loaded From and attempted To, but only
`StepResult.Confirmed` says the Store returned success; false is not proof that
an external commit did not happen.

The rest — guard purity, how `ErrNotPermitted` propagates through nested machines, why `S` and `E`
must be strictly comparable, and why generated tables want `Compile` rather than `MustCompile` — are
documented in full on
[pkg.go.dev](https://pkg.go.dev/github.com/open-ships/statemachine#hdr-Hazards).

## Performance

Apple M1 Pro, Go 1.26, measured with `testing.B.Loop`. Nothing on the flat `Machine`'s successful
path allocates: `Fire` is a map lookup, a guard call and a return, and `Permitted`'s lazy iterator
stays on the stack. A refusal allocates only the error. The state-owning modules intentionally add
synchronization and, where required, eager snapshots or queued work.

```
BenchmarkFireAccepted-10      60591013    21.32 ns/op     0 B/op    0 allocs/op
BenchmarkFireDefaultArm-10    45832159    25.39 ns/op     0 B/op    0 allocs/op
BenchmarkFireRefused-10       11011408   113.90 ns/op    80 B/op    2 allocs/op
BenchmarkRefusalErrorsIs-10  117287949    10.30 ns/op     0 B/op    0 allocs/op
BenchmarkPermitted-10         26448175    47.00 ns/op     0 B/op    0 allocs/op
```

Go 1.26 is what makes `Permitted` free: the `iter.Seq2` it returns closes over the machine, the state
and the data, and through Go 1.25 that closure and the yield function both escaped to the heap — 88 B
and 3 allocations for every call. Escape analysis in 1.26 keeps them on the stack, which is a fourfold
improvement and the reason this module requires it.

## Prior art

Semi-inspired by [looplab/fsm](https://github.com/looplab/fsm), and distilled from it. The flat
Machine's main departures: states and events are typed rather than strings; callbacks are fields on
a row rather than a `map[string]Callback` keyed by magic strings like `"before_open"`; guards and
effects return `error` rather than calling `Event.Cancel`; payloads are a type parameter rather than
`...interface{}`; and the immutable definition does not hold current state. `Instance`, `queued`,
`persist`, and `statechart` add stateful execution without changing that definition.

Requires Go 1.26, which is what keeps `Permitted` allocation-free.

## License

MIT
