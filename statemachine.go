package statemachine

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"

	"github.com/open-ships/statemachine/internal/keycheck"
)

// ErrNotPermitted reports that no transition applied: either no row of the
// table has this From and Event, or every row that does has a Guard that
// declined.
//
// The error [Machine.Next] reports wraps ErrNotPermitted together with every
// reason a Guard returned during the attempt — it implements
// Unwrap() []error — so both of these can hold at once:
//
//	errors.Is(err, statemachine.ErrNotPermitted) // the machine refused
//	errors.Is(err, ErrWindowClosed)             // and this is why
//
// Test for your own reasons first: they are the more precise answer, and the
// only one a caller can act on.
//
// Like every sentinel in Go, this one travels: returning a nested execution's
// refusal from a Guard or Do will make errors.Is report that refusal to its
// caller. Wrapping with %w does not help — it preserves the sentinel. Return a
// different error instead.
var (
	ErrNotPermitted = errors.New("transition not permitted")
	// ErrInvalidKey reports a state or event value that cannot safely be used
	// as a map key because it contains an uncomparable dynamic value.
	ErrInvalidKey = errors.New("statemachine: state or event is not strictly comparable")
)

// A Transition is one row of a transition table: in state From, event Event
// moves an execution to state To.
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
	// Guard must not modify data, perform I/O or panic: [Machine.Next] calls it
	// on rows it does not select, and [Machine.Permitted] calls it while
	// iterating. Nothing enforces this.
	Guard func(ctx context.Context, data T) error

	// Do performs the effect of the transition. It is never run by [Machine.Next]
	// or [Machine.Permitted]. It runs only through a state-owning execution such
	// as [Instance], queued.Runtime, or persist Store-backed execution. A nil Do
	// does nothing.
	//
	// Do runs once after its row is selected and before the execution publishes
	// the destination. A failing Do leaves that execution's state unchanged and
	// does not fall through to the next matching row: the row was already chosen.
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
// state values and the data passed to [Machine.Next] belong to the caller and
// are the caller's to synchronize. The zero Machine has no rows and refuses
// every event.
//
// S and E must be strictly comparable. Go's comparable constraint also admits
// interface-bearing types; Compile rejects them before building any map. A
// zero Machine reports [ErrInvalidKey] from Next for an uncomparable dynamic
// value, while Permitted returns an empty sequence. Prefer distinct defined
// string or integer types for S and E.
type Machine[S, E comparable, T any] struct {
	rows   map[key[S, E]][]Transition[S, E, T]
	events map[S][]E // per state, each event once, in first-mention order

	// strict records that Compile proved S and E free of interface-bearing
	// types, so no dynamic value of either can be an uncomparable map key.
	// Strictness is a property of the type: once proved at compile time, the
	// per-value reflection check in Next and Permitted is unnecessary. The
	// zero Machine has not been through Compile and keeps the per-value check.
	strict bool
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
	if !keycheck.StrictType[S]() {
		return nil, errors.New("statemachine: state type must not contain an interface")
	}
	if !keycheck.StrictType[E]() {
		return nil, errors.New("statemachine: event type must not contain an interface")
	}
	m := &Machine[S, E, T]{
		rows:   make(map[key[S, E]][]Transition[S, E, T], len(transitions)),
		events: make(map[S][]E),
		strict: true,
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

// Next selects the transition for event in state from and reports its
// destination without running its effect.
//
// Next considers the rows whose From and Event match, in table order, and
// selects the first whose Guard is nil or returns nil. It reports that row's
// To. It never calls Do, so discarding its result cannot leave an effect behind.
//
// In every other case Next reports from, unchanged:
//
//	next, err := orders.Next(ctx, order.State, Pay, cmd)
//
// Next is for planning and for caller-owned transitions whose selected row has
// no Do. Use a state-owning execution to perform a transition with an effect;
// assigning Next's destination would deliberately skip that effect.
//
// The error wraps [ErrNotPermitted], and every reason a Guard returned, when no
// row was selected. A selected row always returns a nil error because Next does
// not execute effects.
//
// Next reads the state from the from argument, never from data. A Guard that
// writes a state field on data is writing a second copy that this package
// neither reads nor updates.
//
// Next never reads ctx itself. It passes ctx to Guard, which may honor
// cancellation; a Next under an already-cancelled context still selects a row
// if its Guard does not object.
func (m *Machine[S, E, T]) Next(ctx context.Context, from S, event E, data T) (S, error) {
	transition, err := m.selectTransition(ctx, from, event, data)
	if err != nil {
		return from, err
	}
	return transition.To, nil
}

// fire is the effectful half of flat execution. Keeping it package-private is
// the architectural invariant: a caller-owned state value can ask a Machine
// what comes next, but only a state-owning Instance can perform Do.
func (m *Machine[S, E, T]) fire(ctx context.Context, from S, event E, data T) (S, error) {
	transition, err := m.selectTransition(ctx, from, event, data)
	if err != nil {
		return from, err
	}
	if transition.Do != nil {
		if err := transition.Do(ctx, data); err != nil {
			return from, err
		}
	}
	return transition.To, nil
}

func (m *Machine[S, E, T]) selectTransition(
	ctx context.Context,
	from S,
	event E,
	data T,
) (*Transition[S, E, T], error) {
	if !m.strict && (!keycheck.Value(from) || !keycheck.Value(event)) {
		return nil, ErrInvalidKey
	}
	var reasons []error
	rows := m.rows[key[S, E]{from, event}]
	for index := range rows {
		t := &rows[index]
		if t.Guard != nil {
			if err := t.Guard(ctx, data); err != nil {
				reasons = append(reasons, err)
				continue
			}
		}
		return t, nil
	}
	return nil, &refusal[S, E]{from, event, append([]error{ErrNotPermitted}, reasons...)}
}

// Permitted iterates the events [Machine.Next] would accept in state from for
// data, each paired with the state that selecting it would reach.
//
// An event is yielded at most once, in the order of the first row that mentions
// it for from, and the state yielded with it is exactly the state Next would
// report, because Permitted selects rows by the rule Next uses. An event whose
// every matching row declines is not yielded. No Do runs and nothing changes.
//
// The iterator is lazy: Guards run as it is ranged, not when Permitted is
// called, and they read data at that moment. Do not carry it past the
// synchronization that protects data — collect it first. Stopping early leaves
// the remaining Guards uncalled. If you adapt it with [iter.Pull2], call the
// returned stop function.
//
// Permitted and Next agree on row selection when passed equal data. Data
// assembled for display often omits fields that a Guard consults; pass the same
// construction to both, or keep guard-relevant fields where both can see them.
//
// Use Permitted to offer choices — the buttons on a page, the links in a
// response — not to decide whether to execute. Executing and handling
// [ErrNotPermitted] cannot go stale between the question and the answer.
func (m *Machine[S, E, T]) Permitted(ctx context.Context, from S, data T) iter.Seq2[E, S] {
	return func(yield func(E, S) bool) {
		if !m.strict && !keycheck.Value(from) {
			return
		}
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

// refusal is the error Next and state-owning executions return when no row was
// selected. It holds from and
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
