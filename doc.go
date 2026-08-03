// Package statemachine compiles finite state-machine definitions and runs them
// with caller-owned or package-owned state.
//
// A [Machine] is the flat transition function: an immutable table of
// [Transition] rows, compiled once and safe for use by any number of
// goroutines. It depends only on the standard library.
//
// A Machine does not hold the current state. State is a value the caller owns —
// a field on a struct, a column in a row — and a step is the application of the
// machine to that value:
//
//	var err error
//	order.State, err = orders.Fire(ctx, order.State, Pay, cmd)
//
// This form is useful for database rows and explicit read-modify-write flows:
// restoring an aggregate means passing the loaded state to Fire, and one
// Machine can serve millions of aggregates without creating runtime objects.
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
// a flat machine with row effects. The queued subpackage adds serialized
// run-to-completion cascades, persist places Machine.Fire inside an
// adapter-owned unit of work, and statechart supplies hierarchy, initial
// substates, lifecycle actions, and explicit transition kinds.
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
// command needs. It is passed to [Machine.Fire] rather than stored, so one
// immutable Machine serves every request while still seeing request-scoped
// values. A machine with nothing to carry uses struct{}.
//
// Two rows may share a From and an Event. The first whose Guard applies wins, so
// a trailing unguarded row is a default arm — the semantics of a switch, of
// regexp alternation, and of every routing table. [Compile] rejects the reverse
// order, in which the unguarded row makes the rest of the group dead code.
//
// # Observing transitions
//
// A Machine has no observer or per-state hooks. Fire returns the new state and
// the caller already holds the old state and the event, so every transition is
// visible on the line that performs it. Write the wrapper once per machine and
// put the logging, the metrics and the write there:
//
//	func (s *Service) fire(ctx context.Context, o *Order, e Event, c *Cmd) error {
//		from := o.State
//		next, err := orders.Fire(ctx, from, e, c)
//		o.State = next
//		s.log.InfoContext(ctx, "transition",
//			"id", o.ID, "from", from, "event", e, "to", next, "err", err)
//		return err
//	}
//
// Assigning the returned state is always correct: Fire reports the state it was
// given whenever it reports an error.
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
//   - Discarding [Machine.Fire]'s returned state is never correct. The effect
//     has already run. [Instance.Fire] owns and publishes its state, so callers
//     may discard that state result but must still handle its error.
//   - An Instance is the sole authority for its state. Do not retain another
//     authoritative state field in T; it can diverge on errors and panics.
//   - Instance rejects overlap and same-instance recursive firing. Use the
//     queued subpackage's explicit Enqueue when effects need follow-up events.
//   - State ownership is not effect atomicity. A flat Instance leaves its state
//     unchanged on a refusal, effect error, or callback panic, but arbitrary
//     effects may already be partial. Other execution modules document their
//     own commit points.
//   - S and E must be strictly comparable. Go's comparable constraint admits
//     interface types, and those panic on an uncomparable dynamic value.
//     Prefer distinct defined types for S and E so swapping arguments cannot
//     compile.
//   - A Guard must be pure. It is called for rows that lose, and by Permitted.
//   - A Guard vetoes only if no other row for that From and Event applies.
//   - [ErrNotPermitted] is a sentinel and travels like one. A Guard or Do that
//     fires another machine must not return its refusal, wrapped or otherwise.
//   - Guards and effects see data, which the caller also owns. Recursively
//     calling Machine.Fire for the same caller-owned state loses the nested
//     transition; Instance reports ErrInFlight instead.
package statemachine
