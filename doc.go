// Package statemachine runs finite state machines.
//
// A [Machine] is the transition function of a finite state machine and nothing
// else: an immutable table of [Transition] rows, compiled once, safe for use by
// any number of goroutines. It depends only on the standard library.
//
// A Machine does not hold the current state. State is a value the caller owns —
// a field on a struct, a column in a row — and a step is the application of the
// machine to that value:
//
//	var err error
//	order.State, err = orders.Fire(ctx, order.State, Pay, cmd)
//
// That is the whole idea, and everything else follows from it. Because the
// machine owns no state, restoring one from a database row, running a million
// of them from one table, calling one from many goroutines, and firing one from
// inside another need no support from this package: they are ordinary Go. There
// is no Current, no SetState, no metadata bag, no internal mutex, no
// in-transition error, no deferred-transition protocol, and no observer
// registry, because there is no state inside the library for any of them to
// manage.
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
// There is no observer hook and there are no per-state hooks. Fire returns the
// new state and the caller already holds the old state and the event, so every
// transition is visible on the line that performs it. Write the wrapper once per
// machine and put the logging, the metrics and the write there:
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
// # What this package does not ship
//
// Several things a state machine library usually owns are left to the caller,
// because the table is exported data you wrote and kept, and reading it is
// ordinary Go. Each is a few lines, and each is then in your dialect rather than
// this package's. Runnable versions of all of them are in the examples.
//
// Rendering a diagram is a loop over the table. Checking that every state is
// reachable from a starting state is a fixpoint over the table, and it stays out
// of [Compile] because it needs a starting state, which belongs to the program
// rather than to the table. Fanning one event in from many states is a loop that
// appends rows. Holding the state in an object, if you prefer a method to an
// assignment, is a struct with a mutex — and the lock is then yours, where it
// also protects the rest of the object, which a lock inside this package never
// could.
//
// Behavior shared by every arrow into a state is the one case with no
// comfortable answer. Per-state entry actions are not in the API because they
// would need a second data structure keyed by state, a documented firing order
// against the row's own effect, a failure policy for an entry action that fails
// after that effect already landed, and the internal-versus-external transition
// ambiguity that no FSM library explains successfully. Repeating the effect on
// each inbound row is honest but drifts when a row is added later; a transform
// over the table before compiling it does not drift. Choose one — applying both
// runs the effect twice, and nothing will tell you. The EntryAction example
// shows the transform, and why it must compile and render the table it produced
// rather than the one it was given.
//
// # Hazards
//
//   - Discarding Fire's result is never correct. The effect has already run.
//   - S and E must be strictly comparable. Go's comparable constraint admits
//     interface types, and those panic on an uncomparable dynamic value.
//   - A Guard must be pure. It is called for rows that lose, and by Permitted.
//   - A Guard vetoes only if no other row for that From and Event applies.
//   - [ErrNotPermitted] is a sentinel and travels like one. A Guard or Do that
//     fires another machine must not return its refusal, wrapped or otherwise.
//   - Guards and effects see data, which the caller also owns. Firing the
//     machine that owns the state being advanced, from inside its own effect,
//     loses the nested transition.
package statemachine
