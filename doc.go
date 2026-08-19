// Package statemachine compiles finite state-machine definitions and executes
// them through explicit state owners.
//
// A [Machine] is the flat transition function: an immutable table of
// [Transition] rows, compiled once and safe for use by any number of
// goroutines. It depends only on the standard library.
//
// A Machine does not hold the current state. [Machine.Next] is a pure
// transition query for a caller-owned value: it runs Guards and reports the
// selected destination, but never runs the row's Do effect:
//
//	next, err := orders.Next(ctx, order.State, Submit, cmd)
//
// One Machine can serve millions of aggregates and pure planning queries. To
// run effects, choose the module that owns the resulting state.
//
// When the package should own one in-memory aggregate's current state, use an
// [Instance]:
//
//	run := statemachine.NewInstance(orders, Pending)
//	next, err := run.Fire(ctx, Pay, cmd)
//
// Instance synchronizes inspection, publishes the destination only after the
// selected effect succeeds, rejects overlap and same-instance reentrancy with
// [ErrInFlight], and returns eager affordance snapshots from
// [Instance.Permitted]. Construction is its restoration seam; there is no
// unrestricted state setter.
//
// State ownership does not change definition semantics. This package remains
// a flat machine with row effects, but only a state owner may run those effects.
// The queued subpackage adds serialized run-to-completion cascades, persist
// executes through an adapter-owned unit of work, and statechart supplies hierarchy, initial
// substates, lifecycle actions, and explicit transition kinds. The supervised
// subpackage supplies a separate strict definition with mandatory checks,
// explicit issue and verification, finite time limits, startup reconciliation,
// and first-cause fault latching for safety-adjacent orchestration.
// It is not a safety controller; see the scope and integration obligations at
// https://github.com/open-ships/statemachine/blob/main/SAFETY.md
//
// # Declaring a machine
//
// Declare states and events as defined types with named constants, so a typo is
// a compile error rather than a refusal at run time. Keep the table in a
// package-level variable, so tests and diagram generators read the same rows the
// machine runs:
//
//	type State string
//	type Event string
//
//	const (
//		Draft   State = "draft"
//		Pending State = "pending"
//		Paid    State = "paid"
//	)
//
//	const (
//		Submit Event = "submit"
//		Pay    Event = "pay"
//	)
//
//	// Note the =: an alias, not a defined type, so Compile can infer S, E and T.
//	type row = statemachine.Transition[State, Event, *Cmd]
//
//	var table = []row{
//		{From: Draft, Event: Submit, To: Pending, Guard: hasLines},
//		{From: Pending, Event: Pay, To: Paid, Do: charge},
//	}
//
//	var orders = statemachine.MustCompile(table)
//
// The third type parameter is the value handed to every Guard and Do of that
// machine — the aggregate being transitioned, plus whatever this particular
// command needs. It is passed to [Machine.Next] or a state-owning execution
// rather than stored, so one immutable Machine serves every request while still
// seeing request-scoped values. A machine with nothing to carry uses struct{}.
//
// Two rows may share a From and an Event. The first whose Guard applies wins, so
// a trailing unguarded row is a default arm — the semantics of a switch, of
// regexp alternation, and of every routing table. [Compile] rejects the reverse
// order, in which the unguarded row makes the rest of the group dead code.
//
// # Observing transitions
//
// A Machine has no observer or per-state hooks because Next selects without
// committing anything. State-owning executions can publish committed changes.
// Reading State before Fire is a second, racy operation, and a queued Runtime
// may commit several follow-up transitions before Fire returns.
// [NewInstanceWithObservers], queued.NewWithObservers and
// statechart.Chart.NewWithObservers attach immutable observers at the ownership
// seam. They emit an [Observation] for every committed node exit and entry. A
// flat self-transition changes no position and emits nothing.
//
// Observations describe one execution, not a registry. Construction is
// restoration and emits nothing, so a fleet-wide census must seed its initial
// counts independently before folding observation deltas.
//
// # Working with a flat definition
//
// A flat table is exported data you wrote and kept, so structural operations on
// it remain ordinary Go. Runnable examples show diagram rendering,
// reachability checking, generated fan-in rows, and an entry-action transform.
//
// Rendering a diagram is a loop over the table. Checking that every state is
// reachable from a starting state is a fixpoint over the table, and it stays out
// of [Compile] because it needs a starting state, which belongs to a particular
// workflow rather than to the table. Fanning one event in from many states is a
// loop that appends rows, followed by Compile because generated rows are input.
//
// Behavior shared by every arrow into a state is deliberately not a Machine
// field. It requires entry and exit paths, internal-versus-external semantics,
// and a failure policy after the state commits. For a flat table, a transform
// can add the effect to every inbound row; the EntryAction example shows that
// transform and why the derived table must be compiled and rendered. Use the
// statechart subpackage when lifecycle behavior is part of the definition.
//
// # Hazards
//
//   - [Machine.Next] never runs Do. Effects are available only through a
//     state-owning execution, so discarding a pure query cannot leave an effect
//     behind. Do not use Next to commit an effectful row: that would skip Do.
//     [Instance.Fire] owns and publishes its state, so callers may discard that
//     state result but must still handle its error.
//   - An Instance is the sole authority for its state. Do not retain another
//     authoritative state field in T; it can diverge on errors and panics.
//   - Instance rejects overlap and same-instance recursive firing. Use the
//     queued subpackage's explicit Enqueue when effects need follow-up events.
//   - State ownership is not effect atomicity. A flat Instance leaves its state
//     unchanged on a refusal, effect error, or callback panic, but arbitrary
//     effects may already be partial. Other execution modules document their
//     own commit points.
//   - S and E must be strictly comparable. Compile rejects interface-bearing
//     types because Go's comparable constraint otherwise admits dynamic values
//     that cannot be map keys. Prefer distinct defined types for S and E so
//     swapping arguments cannot compile.
//   - A Guard must be pure. It is called for rows that lose, and by Permitted.
//   - A Guard vetoes only if no other row for that From and Event applies.
//   - [ErrNotPermitted] is a sentinel and travels like one. A Guard or Do that
//     runs another execution must not return its refusal, wrapped or otherwise.
//   - Guards and effects see data, which the caller also owns. Recursively
//     calling [Instance.Fire] on the same execution reports ErrInFlight.
package statemachine
