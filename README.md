# statemachine

Finite state machines in Go. Zero dependencies — standard library only, tests included.

```go
import "github.com/open-ships/statemachine"
```

📖 **[Reference documentation on pkg.go.dev](https://pkg.go.dev/github.com/open-ships/statemachine)**

## The idea

A finite state machine is a partial function from `(state, event)` to `state`. This package is that
function and nothing else.

**The machine does not hold the current state.** State is a value you own — a struct field, a
database column — and a step is the application of the machine to that value:

```go
order.State, err = orders.Fire(ctx, order.State, Pay, cmd)
```

Everything else follows from that one decision. Because the library owns no state, restoring a
machine from a database row, running a million of them from one table, using one from many
goroutines, and firing one from inside another need no support from the package — they are ordinary
Go. There is no `Current`, no `SetState`, no metadata bag, no internal mutex, no in-transition error,
no deferred-transition protocol, and no observer registry, because there is no state inside the
library for any of them to manage.

The whole API is **12 exported identifiers**.

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

**Effects fail with `return err`.** The state advances if and only if `Do` returned `nil`, and the
error comes back to you unwrapped, so `errors.Is` against your own sentinels works with no ceremony:

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

## The API

| | |
|---|---|
| `Transition[S, E comparable, T any]` | one row: `From`, `Event`, `To`, `Guard`, `Do` |
| `Machine[S, E comparable, T any]` | a compiled table; immutable, safe for concurrent use |
| `Compile(transitions)` | build a machine, reporting an unreachable row |
| `MustCompile(transitions)` | the same, panicking — for tables that are program text |
| `Machine.Fire(ctx, from, event, data)` | apply an event; report the state to move to |
| `Machine.Permitted(ctx, from, data)` | iterate the events accepted now, each with its destination |
| `ErrNotPermitted` | the sentinel every refusal wraps |

`T` is the value handed to every `Guard` and `Do` — your aggregate, plus whatever this command needs.
It is passed to `Fire` rather than stored, so one immutable `Machine` serves every request while
still seeing request-scoped values. A machine with nothing to carry uses `struct{}`.

Visualization, reachability checking, `Current`/`SetState`, observer hooks, and per-state entry
actions are deliberately absent: the table is exported data you wrote and kept, and reading it is
ordinary Go. Each is a few lines, in your dialect rather than this package's, and each has a runnable
version in [`example_test.go`](example_test.go). The reasoning for every omission — including the one
genuinely uncomfortable cut, per-state entry actions — is in the [package
documentation](https://pkg.go.dev/github.com/open-ships/statemachine).

## Hazards

**Discarding `Fire`'s result is never correct.** The effect has already run, and neither the compiler
nor `go vet` reports the lost transition.

The rest — guard purity, how `ErrNotPermitted` propagates through nested machines, why `S` and `E`
must be strictly comparable, and why generated tables want `Compile` rather than `MustCompile` — are
documented in full on
[pkg.go.dev](https://pkg.go.dev/github.com/open-ships/statemachine#hdr-Hazards).

## Performance

Apple M1 Pro, Go 1.26, measured with `testing.B.Loop`. Nothing on a successful path allocates:
`Fire` is a map lookup, a guard call and a return, and `Permitted`'s iterator stays on the stack. A
refusal allocates only the error.

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

Semi-inspired by [looplab/fsm](https://github.com/looplab/fsm), and distilled from it. The main
departures: states and events are typed rather than strings; callbacks are fields on a row rather
than a `map[string]Callback` keyed by magic strings like `"before_open"`; guards and effects return
`error` rather than calling `Event.Cancel`; payloads are a type parameter rather than
`...interface{}`; and the machine does not hold the current state, which is what removes most of the
rest.

Requires Go 1.26, which is what keeps `Permitted` allocation-free.

## License

MIT
