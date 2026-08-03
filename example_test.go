package statemachine_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/open-ships/statemachine"
)

// A two-state toggle: the smallest complete machine.
func Example() {
	type State string
	type Event string

	const (
		Off State = "off"
		On  State = "on"
	)
	const Flip Event = "flip"

	light := statemachine.MustCompile([]statemachine.Transition[State, Event, struct{}]{
		{From: Off, Event: Flip, To: On},
		{From: On, Event: Flip, To: Off},
	})

	s := Off
	for range 3 {
		s, _ = light.Fire(context.Background(), s, Flip, struct{}{})
		fmt.Println(s)
	}

	// Output:
	// on
	// off
	// on
}

// --- The order lifecycle shared by the examples below -------------------------

// State and Event are defined types with named constants, so a misspelled state
// or event is a compile error rather than a refusal at run time.
type State string

const (
	Draft       State = "draft"
	Pending     State = "pending"
	Paid        State = "paid"
	Shipped     State = "shipped"
	Backordered State = "backordered"
	Delivered   State = "delivered"
	Cancelled   State = "cancelled"
	Refunded    State = "refunded"
)

type Event string

const (
	Submit  Event = "submit"
	Pay     Event = "pay"
	Ship    Event = "ship"
	Deliver Event = "deliver"
	Cancel  Event = "cancel"
	Refund  Event = "refund"
)

// Order is the aggregate used by the caller-owned Machine examples. In that
// execution model State is an ordinary field: restoration means loading it and
// passing it to Fire.
type Order struct {
	ID      string
	State   State
	Lines   int
	InStock bool
	Charged bool
}

// Cmd is the third type parameter: the aggregate plus whatever this particular
// command needs. It is passed to Fire rather than stored, so one immutable
// Machine serves every request while still seeing request-scoped values.
type Cmd struct {
	Order *Order
	Card  string
	Log   []string
}

// Reasons a guard can give. A caller matches on these to answer 422 rather than
// 409 — the distinction a boolean guard could never carry.
var (
	ErrNoLines      = errors.New("order has no lines")
	ErrWindowClosed = errors.New("return window has closed")
	ErrDeclined     = errors.New("card declined")

	// errNotStocked routes Paid+Ship to the backorder arm. In a state without
	// a fallback Ship row, the same reason would surface with the refusal.
	errNotStocked = errors.New("out of stock")
)

func orderHasLines(_ context.Context, c *Cmd) error {
	if c.Order.Lines == 0 {
		return ErrNoLines
	}
	return nil
}

func orderInStock(_ context.Context, c *Cmd) error {
	if !c.Order.InStock {
		return errNotStocked
	}
	return nil
}

func charge(_ context.Context, c *Cmd) error {
	if c.Card == "" {
		return ErrDeclined
	}
	c.Order.Charged = true
	c.Log = append(c.Log, "charged")
	return nil
}

// Row keeps the table readable. Note the =: an alias, not a defined type, so
// Compile can still infer S, E and T.
type Row = statemachine.Transition[State, Event, *Cmd]

// orderTable is a package variable, so tests, the diagram renderer and the
// reachability check below all read the same rows the machine runs.
var orderTable = []Row{
	{From: Draft, Event: Submit, To: Pending, Guard: orderHasLines},
	{From: Draft, Event: Cancel, To: Cancelled},

	{From: Pending, Event: Pay, To: Paid, Do: charge},
	{From: Pending, Event: Cancel, To: Cancelled},

	// Two rows share Paid+Ship. The first whose guard applies wins; the second
	// is the unguarded default arm. Compile rejects the reverse order.
	{From: Paid, Event: Ship, To: Shipped, Guard: orderInStock},
	{From: Paid, Event: Ship, To: Backordered},
	{From: Paid, Event: Cancel, To: Refunded},

	{From: Backordered, Event: Ship, To: Shipped, Guard: orderInStock},
	{From: Backordered, Event: Cancel, To: Refunded},

	{From: Shipped, Event: Deliver, To: Delivered},
	{From: Delivered, Event: Refund, To: Refunded},
}

var lifecycle = statemachine.MustCompile(orderTable)

// Fire reports where to go next; the caller owns the state and writes it down.
func ExampleMachine_Fire() {
	ctx := context.Background()
	o := &Order{ID: "A1", State: Draft, Lines: 2}

	var err error
	o.State, err = lifecycle.Fire(ctx, o.State, Submit, &Cmd{Order: o})
	fmt.Println(o.State, err)

	// A guard declined and no other row applied: the state is unchanged and the
	// reason travels with the refusal.
	empty := &Order{ID: "A2", State: Draft}
	next, err := lifecycle.Fire(ctx, empty.State, Submit, &Cmd{Order: empty})
	fmt.Println(next, err)
	fmt.Println("refused:", errors.Is(err, statemachine.ErrNotPermitted),
		"| why:", errors.Is(err, ErrNoLines))

	// An effect failed: Fire returns the effect's own error, unwrapped, and the
	// state does not advance.
	next, err = lifecycle.Fire(ctx, o.State, Pay, &Cmd{Order: o})
	fmt.Println(next, err)
	fmt.Println("refused:", errors.Is(err, statemachine.ErrNotPermitted),
		"| declined:", errors.Is(err, ErrDeclined))

	// Output:
	// pending <nil>
	// draft statemachine: submit in state draft: transition not permitted: order has no lines
	// refused: true | why: true
	// pending card declined
	// refused: false | declined: true
}

// Permitted answers "what may happen next", following the same guards Fire
// follows, so a rendered affordance and the server that receives it cannot
// disagree about where an event leads.
func ExampleMachine_Permitted() {
	ctx := context.Background()

	for _, inStock := range []bool{true, false} {
		o := &Order{ID: "A1", State: Paid, InStock: inStock}
		fmt.Printf("in stock %-5v:", inStock)
		for event, to := range lifecycle.Permitted(ctx, o.State, &Cmd{Order: o}) {
			fmt.Printf(" %v->%v", event, to)
		}
		fmt.Println()
	}

	// Output:
	// in stock true : ship->shipped cancel->refunded
	// in stock false: ship->backordered cancel->refunded
}

// Compile rejects the one defect a table can have that this package can detect:
// a row that can never be selected.
func ExampleCompile() {
	_, err := statemachine.Compile([]Row{
		{From: Paid, Event: Ship, To: Backordered},                  // unguarded
		{From: Paid, Event: Ship, To: Shipped, Guard: orderInStock}, // dead
	})
	fmt.Println(err)

	// Output:
	// statemachine: transitions[1] (paid on ship) is unreachable: transitions[0] has the same From and Event and no Guard
}

// Classifying a failure is a switch on errors.Is. Guard reasons are tested
// first: they are also wrapped in ErrNotPermitted, and they are the more
// precise answer.
func Example_httpStatus() {
	classify := func(err error) int {
		switch {
		case err == nil:
			return http.StatusOK
		case errors.Is(err, ErrNoLines), errors.Is(err, ErrWindowClosed):
			return http.StatusUnprocessableEntity // the guard explained itself
		case errors.Is(err, statemachine.ErrNotPermitted):
			return http.StatusConflict // wrong state; retrying will not help
		case errors.Is(err, ErrDeclined):
			return http.StatusPaymentRequired // the effect failed
		default:
			return http.StatusInternalServerError
		}
	}

	ctx := context.Background()
	for _, o := range []*Order{
		{State: Draft, Lines: 2}, // submits cleanly
		{State: Draft},           // no lines
		{State: Delivered},       // wrong state for submit
	} {
		_, err := lifecycle.Fire(ctx, o.State, Submit, &Cmd{Order: o})
		fmt.Println(classify(err))
	}

	// Output:
	// 200
	// 422
	// 409
}

// Rendering a diagram is a loop over the table you already wrote, in whatever
// dialect you like. This is why no Visualize function and no Transitions
// accessor are in the API.
func Example_mermaid() {
	var b strings.Builder
	b.WriteString("stateDiagram-v2\n")
	for _, t := range orderTable {
		guarded := ""
		if t.Guard != nil {
			guarded = " [guarded]"
		}
		fmt.Fprintf(&b, "    %v --> %v: %v%s\n", t.From, t.To, t.Event, guarded)
	}
	fmt.Print(b.String())

	// Output:
	// stateDiagram-v2
	//     draft --> pending: submit [guarded]
	//     draft --> cancelled: cancel
	//     pending --> paid: pay
	//     pending --> cancelled: cancel
	//     paid --> shipped: ship [guarded]
	//     paid --> backordered: ship
	//     paid --> refunded: cancel
	//     backordered --> shipped: ship [guarded]
	//     backordered --> refunded: cancel
	//     shipped --> delivered: deliver
	//     delivered --> refunded: refund
}

// Reachability is a fixpoint over the table. It stays out of Compile because it
// needs a starting state, which belongs to the program rather than to the table.
//
// Note that this is a question about the table, not about any particular order,
// so it is answered by reading rows — not by ranging Permitted, which consults
// guards and would give a different answer for two orders in the same state.
func Example_reachability() {
	reachable := map[State]bool{Draft: true}
	for grew := true; grew; {
		grew = false
		for _, t := range orderTable {
			if reachable[t.From] && !reachable[t.To] {
				reachable[t.To], grew = true, true
			}
		}
	}

	var orphans []string
	for _, s := range []State{Pending, Paid, Shipped, Backordered, Delivered, Cancelled, Refunded} {
		if !reachable[s] {
			orphans = append(orphans, string(s))
		}
	}
	slices.Sort(orphans)
	fmt.Println("unreachable from draft:", orphans)

	// Output:
	// unreachable from draft: []
}

// Behavior shared by every arrow into a state has no dedicated API. Repeating
// the effect on each inbound row drifts the moment someone adds a row, so
// derive the table instead — and then compile and render the derived table,
// never the one it was given.
//
// The transform must choose whether the shared effect runs before or after the
// row's own effect, and whether a self-transition re-runs it. Those are the same
// questions per-state hooks would force the library to answer; deriving the
// table moves them to the one place that knows the domain.
func Example_entryAction() {
	// releaseHold runs on entry to Cancelled and Refunded, however we got there.
	releaseHold := func(_ context.Context, c *Cmd) error {
		c.Log = append(c.Log, "released hold")
		return nil
	}

	withEntry := func(table []Row, on map[State]func(context.Context, *Cmd) error) []Row {
		out := make([]Row, 0, len(table))
		for _, t := range table {
			entry, ok := on[t.To]
			// A self-transition never leaves the state, so it does not re-enter it.
			if ok && t.To != t.From {
				own := t.Do
				t.Do = func(ctx context.Context, c *Cmd) error {
					if own != nil {
						if err := own(ctx, c); err != nil {
							return err // the row's own effect gates entry
						}
					}
					return entry(ctx, c)
				}
			}
			out = append(out, t)
		}
		return out
	}

	derived := withEntry(orderTable, map[State]func(context.Context, *Cmd) error{
		Cancelled: releaseHold,
		Refunded:  releaseHold,
	})
	machine := statemachine.MustCompile(derived) // compile the derived table

	ctx := context.Background()
	for _, from := range []State{Draft, Pending, Paid} {
		o := &Order{State: from}
		c := &Cmd{Order: o}
		o.State, _ = machine.Fire(ctx, o.State, Cancel, c)
		fmt.Println(from, "->", o.State, c.Log)
	}

	// Output:
	// draft -> cancelled [released hold]
	// pending -> cancelled [released hold]
	// paid -> refunded [released hold]
}

// Generated rows are input, not program text, so they go through Compile rather
// than MustCompile. A transform must also preserve Compile's ordering rule: do
// not append a generated default after a default that is already present.
func Example_fanIn() {
	rows := slices.Clone(orderTable)
	type key struct {
		from  State
		event Event
	}
	declared := make(map[key]bool, len(rows))
	for _, row := range rows {
		declared[key{row.From, row.Event}] = true
	}
	for _, from := range []State{Draft, Pending, Paid, Backordered, Shipped} {
		if !declared[key{from, Cancel}] {
			rows = append(rows, Row{From: from, Event: Cancel, To: Cancelled})
		}
	}

	machine, err := statemachine.Compile(rows)
	if err != nil {
		fmt.Println("compile:", err)
		return
	}
	next, err := machine.Fire(context.Background(), Shipped, Cancel, &Cmd{})
	fmt.Println(Shipped, "->", next, err)

	// Output:
	// shipped -> cancelled <nil>
}

// An Instance owns one aggregate's state. It centralizes synchronization,
// commit-after-effect ordering, overlap rejection, and eager affordance
// snapshots while the immutable Machine remains shared.
func ExampleInstance() {
	type Cart struct {
		Lines   int
		InStock bool
		Charged bool
	}
	type Command struct {
		Cart *Cart
		Card string
	}
	type InstanceRow = statemachine.Transition[State, Event, *Command]

	machine := statemachine.MustCompile([]InstanceRow{
		{
			From: Draft, Event: Submit, To: Pending,
			Guard: func(_ context.Context, command *Command) error {
				if command.Cart.Lines == 0 {
					return ErrNoLines
				}
				return nil
			},
		},
		{
			From: Pending, Event: Pay, To: Paid,
			Do: func(_ context.Context, command *Command) error {
				if command.Card == "" {
					return ErrDeclined
				}
				command.Cart.Charged = true
				return nil
			},
		},
		{
			From: Paid, Event: Ship, To: Shipped,
			Guard: func(_ context.Context, command *Command) error {
				if !command.Cart.InStock {
					return errNotStocked
				}
				return nil
			},
		},
		{From: Paid, Event: Ship, To: Backordered},
		{From: Paid, Event: Cancel, To: Refunded},
	})

	ctx := context.Background()
	cart := &Cart{Lines: 1, InStock: true}
	instance := statemachine.NewInstance(machine, Draft)

	_, err := instance.Fire(ctx, Submit, &Command{Cart: cart})
	fmt.Println(err)
	_, err = instance.Fire(ctx, Pay, &Command{Cart: cart, Card: "4242"})
	fmt.Println(err)

	// Permitted runs all guards before returning, so the snapshot is safe to
	// collect later. State lives only in instance; neither Cart nor Command
	// contains a second copy.
	var actions []Event
	for event := range instance.Permitted(ctx, &Command{Cart: cart}) {
		actions = append(actions, event)
	}
	fmt.Println(instance.State(), actions)

	// Output:
	// <nil>
	// <nil>
	// paid [ship cancel]
}
