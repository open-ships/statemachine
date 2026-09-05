# statemachine

Finite state machines in Go. The root library and its unit tests use only the standard library.
The optional SQLite integration tests live in a separate module.

Version **1.4.0 is the first stable release**. Earlier tags are prerelease history;
see [the migration guide](MIGRATION.md) before upgrading. The module path stays the same.

```go
import "github.com/open-ships/statemachine"
```

📖 **[Reference documentation on pkg.go.dev](https://pkg.go.dev/github.com/open-ships/statemachine)**

## The idea

A finite state machine is a partial function from `(state, event)` to `state`. This package keeps
that function as its core and builds optional state owners around it.

**A `Machine` does not hold the current state.** It is an immutable compiled definition. `Next`
answers a pure question about a value you own and never runs the selected row's effect:

```go
next, err := orders.Next(ctx, order.State, Submit, cmd)
```

Use `Next` for planning or a row with no `Do`. To perform an effectful row, use
an `Instance`, queued Runtime, or Store-backed execution; assigning the `Next`
result would intentionally skip its effect.

One definition can serve a million aggregates and any number of goroutines. When the package should
own execution state instead, construct one `Instance` per aggregate. The [`queued`](queued) package
adds serialized run-to-completion execution, [`persist`](persist) runs the flat definition inside an
adapter-owned unit of work, and [`statechart`](statechart) supplies hierarchy and lifecycle actions.
A transactional `persist.Store` can pass its transaction through to effects. The
[`supervised`](supervised) package adds a strict Issue–Verify protocol, mandatory checks, finite
execution limits, startup reconciliation, and latched faults for safety-adjacent orchestration.
These modules remain separate because they have different concurrency, commit, and failure semantics.

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
	s, _ = light.Next(context.Background(), s, Flip, struct{}{})
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
_, err := orders.Next(ctx, Delivered, Refund, cmd)

errors.Is(err, statemachine.ErrNotPermitted) // true: the machine refused    -> 409
errors.Is(err, ErrWindowClosed)              // true: and this is why        -> 422
```

That is the whole 409-versus-422 story, with no second error type and no `errors.As`.

**Effects run only behind a state owner.** `Machine.Next` never calls `Do`. `Instance.Fire`, a queued
runtime, or Store-backed execution runs the selected effect and publishes the destination only when
`Do` returns `nil`. An Instance returns a `Do` error unchanged; other state owners can also
report scheduling, persistence, or observation failures:

```go
{From: Pending, Event: Pay, To: Paid, Do: func(ctx context.Context, c *Cmd) error {
	return c.gateway.Charge(ctx, c.Order.ID, c.Order.Cents) // fails -> stays Pending
}},
```

**Affordances come from the same selection rule as execution.** They describe the state and data
used for the query; those values can change before a click is handled, so the handler must still
check its execution result:

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
| `Machine.Next(ctx, from, event, data)` | select and report a destination without running `Do` |
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
by one event, `Remaining == 0` closes that Step, and `At` timestamps its committed
publication. Context and `T` are passed
to the typed `Observer` separately. Observers run synchronously without an
execution lock, but in isolation: an observer panic or `runtime.Goexit` cannot
reverse the committed transition and is returned as a post-commit error. Flat self-transitions change no position and are
observation-silent. A shared observer can be called concurrently by different
executions and must synchronize its own census or sink.

`T` is the value handed to every `Guard` and `Do` — your aggregate, plus whatever this command needs.
It is passed to `Next` or the state-owning execution rather than stored, so one immutable `Machine`
serves every request while still seeing request-scoped values. A machine with nothing to carry uses
`struct{}`.

Visualization and reachability checking remain ordinary loops over the flat table. Per-state entry
and exit actions do not belong to a flat `Machine`; use the `statechart` package when those semantics
are required. Runnable flat-machine examples are in [`example_test.go`](example_test.go).

## Choosing an execution model

| Need | Module | State and concurrency semantics |
|---|---|---|
| Pure planning or explicit assignment with no effects | `Machine.Next` | caller-owned value; never runs `Do` |
| One in-memory aggregate | `Instance` | owned state; overlapping fire fails fast |
| Follow-up events and FIFO serialization | `queued.Runtime` | owned state; each root run drains to completion |
| Database state and outbox work | `persist.Fire` | transactional when the Store supplies a transaction; never auto-retried |
| Hierarchy, initial substates, entry/exit, reentry | `statechart` | immutable chart plus one stateful instance per aggregate |
| Explicit issue/verification and latched faults | `supervised.Supervisor` | requires reconciliation; normal external commits follow Verify, logical commits occur in Issue |

The supervised module is not a safety-rated controller. It cannot terminate a callback that ignores
cancellation or replace independent emergency stop, safe torque off, guarding, collision protection,
or human-presence separation. Its detailed operating assumptions and prohibited uses are in
[`SAFETY.md`](SAFETY.md). Its strict Machine exposes states, events, transition metadata, explicit
refusals, and deterministic Graphviz DOT output without exposing callback values.

Supervisor operations return `(Result, error)` and use `(ExecutionID, IncarnationID, Sequence)`
Attempt identities. Every restore creates a fresh random IncarnationID, even from the same saved
Snapshot, so an unsaved counter cannot reuse a previous command identity. `NewWithOptions` adds a
Clock, a durable pre-Issue Journal, and a lifecycle Recorder. The newest causal records, including
asynchronous expiry and secondary causes, remain available through `Records` after recovery.
Within one Supervisor, Journal and Recorder delivery each admit one callback at a time and retain ownership after a
caller timeout until the callback actually ends. Busy delivery is visible in `Status`; it is not
queued or automatically retried.
After a Fault, explicit Adjudicate can retain state, adopt an in-doubt destination, or override
to a declared state through reconciliation. Adoption records that decision without claiming a successful Verify.

A queued callback schedules same-runtime follow-ups with `runtime.Enqueue` and the context it was
given. Naming the Runtime makes cross-Runtime mistakes return `ErrWrongRuntime`; finite limits bound
outstanding roots and cumulative Run work. `queued.NewWithOptions` combines limits and observers;
`LastPanic` retains the latest originating stack while Fire re-panics the original value. It must never synchronously call `Runtime.Fire`;
replacing that context defeats deadlock detection.

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

**`Machine.Next` never runs `Do`.** Effectful execution is available only through a state owner, so
discarding a pure query cannot run that effect. Guards must remain pure; the library cannot prevent
application code from performing I/O inside a Guard. A state-owning `Instance.Fire` may
discard its returned state because the Instance has already committed it, but its error still must
be handled.

An `Instance` or queued runtime is the sole owner of its state. Do not also keep an authoritative
copy in the value passed as `T`. Effects can still be partial: the state owner guarantees its state
transition, not rollback of arbitrary I/O. Its synchronization protects the owned state, not other
mutable fields inside `T`.

For persistence, a getter and setter are not a transaction. A `persist.Store` must use a conditional
write and must invoke the transition callback exactly once after a successful load. A transactional
database Store should put aggregate changes and an outbox record in the same unit of work. Only work
performed through that transaction is atomic. A conflict discovered after an effect is not retried;
an application retry must reload and use a stable idempotency key. `persist.Fire` and `persist.Step`
detect repeated or incomplete callbacks when Update returns, and reject successful Store results
that omit a callback, hide a callback failure, or report a different destination. They never execute a repeated transition step. Use the reusable
[`persist/persisttest`](persist/persisttest) conformance suite and the
[SQLite integration example](integration/sqlite) to verify an adapter against a real transaction.
`persist.Step` additionally reports the loaded From and attempted To, but only
`StepResult.Confirmed` says the Store returned success and passed the detectable contract checks;
false is not proof that
an external commit did not happen.

The rest — guard purity, how `ErrNotPermitted` propagates through nested machines, why `S` and `E`
must be strictly comparable and equal to themselves (NaN-bearing keys are rejected), and why generated tables want `Compile` rather than `MustCompile` — are
documented in full on
[pkg.go.dev](https://pkg.go.dev/github.com/open-ships/statemachine#hdr-Hazards).

## Performance

Apple M1 Pro, Go 1.26, measured with `testing.B.Loop`. The successful flat `Machine` paths in
these benchmarks allocate zero bytes: `Next` performs selection, and `Permitted`'s lazy iterator
stays on the stack. Refusal costs include constructing its error. Application callbacks and different
escaping uses can allocate. The state-owning modules intentionally add
synchronization and, where required, eager snapshots or queued work.

```
BenchmarkNextAccepted-10      44001490    27.18 ns/op     0 B/op    0 allocs/op
BenchmarkNextDefaultArm-10    61790773    19.42 ns/op     0 B/op    0 allocs/op
BenchmarkNextRefused-10       25778685    46.09 ns/op    80 B/op    2 allocs/op
BenchmarkRefusalErrorsIs-10  132888273     9.03 ns/op     0 B/op    0 allocs/op
BenchmarkPermitted-10         30415488    39.53 ns/op     0 B/op    0 allocs/op
```

The returned `iter.Seq2` closes over the machine, state, and data. In the earlier Go 1.25
measurements of this workload, the closure and yield function escaped to the heap — 88 B and
3 allocations per call. Go 1.26 keeps these non-escaping uses on the stack. Retaining an iterator
beyond the calling function can have different allocation costs.

Run `scripts/check.sh` for the shared local/CI assurance checks and `scripts/benchmark.sh`
for repeated benchmark samples. Fire benchmarks measure one call per operation; cascade,
contention, and observer-count benchmarks name their workload explicitly. Compare samples on
the same machine and toolchain. See [CONTRIBUTING.md](CONTRIBUTING.md) for fuzzing and release checks.

## Prior art

Semi-inspired by [looplab/fsm](https://github.com/looplab/fsm), and distilled from it. The flat
Machine's main departures: states and events are typed rather than strings; callbacks are fields on
a row rather than a `map[string]Callback` keyed by magic strings like `"before_open"`; guards and
effects return `error` rather than calling `Event.Cancel`; payloads are a type parameter rather than
`...interface{}`; and the immutable definition does not hold current state. `Instance`, `queued`,
`persist`, and `statechart` add stateful execution without changing that definition.

Requires Go 1.26. Allocation behavior depends on compiler escape analysis and iterator usage.

## License

MIT
