package statemachine

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"
)

// ErrNotPermitted reports that no transition applied: either no row of the
// table has this From and Event, or every row that does has a Guard that
// declined.
//
// The error [Machine.Fire] reports wraps ErrNotPermitted together with every
// reason a Guard returned during the attempt — it implements
// Unwrap() []error — so both of these can hold at once:
//
//	errors.Is(err, statemachine.ErrNotPermitted) // the machine refused
//	errors.Is(err, ErrWindowClosed)             // and this is why
//
// Test for your own reasons first: they are the more precise answer, and the
// only one a caller can act on.
//
// Like every sentinel in Go, this one travels: a Guard or Do that fires
// another Machine and returns that call's refusal will make errors.Is report a
// refusal for a transition this Machine permitted. Wrapping with %w does not
// help — it preserves the sentinel. Return a different error instead.
var ErrNotPermitted = errors.New("transition not permitted")

// A Transition is one row of a transition table: in state From, event Event
// moves the machine to state To.
//
// Declare an alias to keep tables readable — note the =, which makes it an
// alias rather than a defined type, so that [Compile] can still infer S, E and
// T:
//
//	type row = statemachine.Transition[State, Event, *Cmd]
//
// A Transition is not comparable: it has function fields, so ==,
// slices.Contains and use as a map key are all compile errors.
//
// The zero Transition is a self-transition on the zero state and the zero
// event, with no Guard and no effect.
type Transition[S, E comparable, T any] struct {
	// From is the state this transition leaves.
	From S

	// Event is the event that triggers it.
	Event E

	// To is the state this transition enters. To equal to From is a
	// self-transition, fired like any other row.
	To S

	// Guard reports whether this row applies to data right now: nil to apply,
	// any error to decline, and that error is the reason.
	//
	// Declining is not a failure. It removes this row from consideration and
	// the next matching row is tried; only when no row is left does the reason
	// reach the caller, wrapped in [ErrNotPermitted]. A nil Guard always
	// applies, so the last row for a From and Event may be an unguarded
	// default.
	//
	// Whether a Guard vetoes or merely routes is therefore not a property of
	// the Guard: it depends on whether another row for the same From and Event
	// applies. A condition that must block the event outright has to appear on
	// every row of that group — putting it first is not enough, because a later
	// unguarded row will still win.
	//
	// Return a nil error, never a typed nil: a (*MyError)(nil) returned as an
	// error is non-nil, so the row silently declines and the next one wins.
	//
	// Guard must not modify data, perform I/O or panic: [Machine.Fire] calls it
	// on rows it does not select, and [Machine.Permitted] calls it while
	// iterating. Nothing enforces this.
	Guard func(ctx context.Context, data T) error

	// Do performs the effect of the transition. It runs once, after this row is
	// selected and before Fire reports the new state, so the state advances if
	// and only if Do returned nil. A nil Do does nothing.
	//
	// Do is the only thing that can fail a row that was selected, and the error
	// it returns is the error Fire returns, unwrapped. A failing Do does not
	// fall through to the next matching row: the row was already chosen.
	Do func(ctx context.Context, data T) error
}

// key identifies the group of rows a (state, event) pair selects from.
type key[S, E comparable] struct {
	from  S
	event E
}

// A Machine is a compiled transition table.
//
// A Machine is immutable and safe for concurrent use by any number of
// goroutines. This package takes no locks, so no deadlock originates here; the
// state values and the data passed to [Machine.Fire] belong to the caller and
// are the caller's to synchronize. The zero Machine has no rows and refuses
// every event.
//
// S and E must be strictly comparable. Go's comparable constraint is also
// satisfied by interface types, and a Machine whose S or E is an interface
// panics when a value's dynamic type is not comparable — the same panic a map
// index gives. Use a defined string or integer type.
type Machine[S, E comparable, T any] struct {
	rows   map[key[S, E]][]Transition[S, E, T]
	events map[S][]E // per state, each event once, in first-mention order
}

// Compile builds a [Machine] from a transition table. It copies transitions, so
// later changes to the slice do not affect the Machine; the Guard and Do
// function values are shared, not copied. Because the copy is taken eagerly,
// rows appended to the table afterwards — including from a func init, which
// runs after package-level variable initialization — are not in the Machine.
// Build the table in one expression.
//
// Compile reports an error for the one defect a table can have that this
// package can detect: a row that can never be selected, because an earlier row
// with the same From and Event has no Guard. Everything else is legal — an
// empty table, a state with no outgoing row (a terminal state), and a state
// named only by To, since this package is never told where a run begins and so
// cannot tell an unreachable state from a state you enter another way.
//
// Compiling an untyped nil needs explicit type arguments; a nil slice of the
// right type does not:
//
//	var empty []row
//	m, err := statemachine.Compile(empty)
func Compile[S, E comparable, T any](transitions []Transition[S, E, T]) (*Machine[S, E, T], error) {
	m := &Machine[S, E, T]{
		rows:   make(map[key[S, E]][]Transition[S, E, T], len(transitions)),
		events: make(map[S][]E),
	}
	// Once a group has an unguarded row, every later row in that group is dead.
	unguarded := make(map[key[S, E]]int, len(transitions))

	for i, t := range transitions {
		k := key[S, E]{t.From, t.Event}
		if j, dead := unguarded[k]; dead {
			return nil, fmt.Errorf(
				"statemachine: transitions[%d] (%v on %v) is unreachable: transitions[%d] has the same From and Event and no Guard",
				i, t.From, t.Event, j)
		}
		if t.Guard == nil {
			unguarded[k] = i
		}
		if _, seen := m.rows[k]; !seen {
			m.events[t.From] = append(m.events[t.From], t.Event)
		}
		m.rows[k] = append(m.rows[k], t)
	}
	return m, nil
}

// MustCompile is like [Compile] but panics if the table is invalid. It is for
// tables written as literals, which are program text rather than input:
//
//	var orders = statemachine.MustCompile(table)
//
// A table assembled at run time — from configuration, or by generating rows in
// a loop — is input. Use [Compile] and handle the error, or a table that
// happens to contain a dead row takes the process down at import time.
func MustCompile[S, E comparable, T any](transitions []Transition[S, E, T]) *Machine[S, E, T] {
	m, err := Compile(transitions)
	if err != nil {
		panic(err)
	}
	return m
}

// Fire applies event to state from and reports the state to move to.
//
// Fire considers the rows whose From and Event match, in table order, and
// selects the first whose Guard is nil or returns nil. It runs that row's Do
// and, if Do returns nil, reports the row's To. A failing Do does not fall
// through to the next row.
//
// In every other case Fire reports from, unchanged, so assigning the result is
// always correct:
//
//	var err error
//	order.State, err = orders.Fire(ctx, order.State, Pay, cmd)
//
// Discarding it never is: the effect has already run, and neither the compiler
// nor go vet reports the lost transition.
//
// The error wraps [ErrNotPermitted], and every reason a Guard returned, when no
// row was selected; otherwise it is exactly the error Do returned.
//
// Fire reads the state from the from argument, never from data. A Guard or Do
// that writes a state field on data is writing a second copy that this package
// neither reads nor updates: the write is discarded on success and left behind
// on failure.
//
// Fire never reads ctx. It passes ctx to Guard and Do, which may honor
// cancellation themselves; a Fire under an already-cancelled context still
// transitions if its Guard and Do do not object.
//
// A Guard or Do may call Fire — on this Machine or another — with no
// restriction, because there is no lock and no in-flight state. What a nested
// call reaches is returned only to the nested caller: the outer Fire still
// reports its own row's To on success, and reports from on failure, discarding
// any state the nested call reached while its effects stand. Nest only across
// distinct aggregates, and never fire the machine that owns the state the
// current transition is advancing.
func (m *Machine[S, E, T]) Fire(ctx context.Context, from S, event E, data T) (S, error) {
	var reasons []error
	for _, t := range m.rows[key[S, E]{from, event}] {
		if t.Guard != nil {
			if err := t.Guard(ctx, data); err != nil {
				reasons = append(reasons, err)
				continue
			}
		}
		if t.Do != nil {
			if err := t.Do(ctx, data); err != nil {
				return from, err
			}
		}
		return t.To, nil
	}
	return from, &refusal[S, E]{from, event, append([]error{ErrNotPermitted}, reasons...)}
}

// Permitted iterates the events [Machine.Fire] would accept in state from for
// data, each paired with the state that firing it would reach.
//
// An event is yielded at most once, in the order of the first row that mentions
// it for from, and the state yielded with it is exactly the state Fire would
// report, because Permitted selects rows by the rule Fire uses. An event whose
// every matching row declines is not yielded. No Do runs and nothing changes.
//
// The iterator is lazy: Guards run as it is ranged, not when Permitted is
// called, and they read data at that moment. Do not carry it past the
// synchronization that protects data — collect it first. Stopping early leaves
// the remaining Guards uncalled. If you adapt it with [iter.Pull2], call the
// returned stop function.
//
// Permitted and Fire agree on row selection when passed equal data. They can
// still disagree on outcome: a Do may fail, and data assembled for display
// often omits fields — a request payload, an open transaction — that a Guard
// consults. Pass the same construction to both, or keep guard-relevant fields
// where both can see them.
//
// Use Permitted to offer choices — the buttons on a page, the links in a
// response — not to decide whether to fire. Firing and handling
// [ErrNotPermitted] cannot go stale between the question and the answer.
func (m *Machine[S, E, T]) Permitted(ctx context.Context, from S, data T) iter.Seq2[E, S] {
	return func(yield func(E, S) bool) {
		for _, event := range m.events[from] {
			for _, t := range m.rows[key[S, E]{from, event}] {
				if t.Guard != nil && t.Guard(ctx, data) != nil {
					continue
				}
				if !yield(event, t.To) {
					return
				}
				break
			}
		}
	}
}

// refusal is the error Fire returns when no row was selected. It holds from and
// event rather than a formatted string so that the common path — errors.Is,
// without ever printing the error — does not format one, and it holds its
// unwrap slice rather than rebuilding it on every errors.Is call.
type refusal[S, E comparable] struct {
	from   S
	event  E
	unwrap []error // ErrNotPermitted, followed by each reason in table order
}

func (r *refusal[S, E]) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "statemachine: %v in state %v: %s", r.event, r.from, ErrNotPermitted.Error())
	for i, reason := range r.unwrap[1:] {
		if i == 0 {
			b.WriteString(": ")
		} else {
			b.WriteString("; ")
		}
		b.WriteString(reason.Error())
	}
	return b.String()
}

func (r *refusal[S, E]) Unwrap() []error { return r.unwrap }
