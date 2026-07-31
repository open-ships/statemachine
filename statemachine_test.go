package statemachine_test

import (
	"context"
	"errors"
	"iter"
	"maps"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/open-ships/statemachine"
)

type state string

const (
	draft     state = "draft"
	pending   state = "pending"
	paid      state = "paid"
	shipped   state = "shipped"
	backorder state = "backordered"
	cancelled state = "cancelled"
)

type event string

const (
	submit event = "submit"
	pay    event = "pay"
	ship   event = "ship"
	cancel event = "cancel"
)

// data is the T of the test machines: an aggregate plus a trace of the effects
// that ran, so tests can assert what happened as well as where it ended up.
type data struct {
	lines   int
	inStock bool
	trace   []string
}

type row = statemachine.Transition[state, event, *data]

var (
	errNoLines    = errors.New("order has no lines")
	errOutOfStock = errors.New("out of stock")
	errDeclined   = errors.New("card declined")
)

func hasLines(_ context.Context, d *data) error {
	if d.lines == 0 {
		return errNoLines
	}
	return nil
}

func inStock(_ context.Context, d *data) error {
	if !d.inStock {
		return errOutOfStock
	}
	return nil
}

func record(name string) func(context.Context, *data) error {
	return func(_ context.Context, d *data) error {
		d.trace = append(d.trace, name)
		return nil
	}
}

func fail(err error) func(context.Context, *data) error {
	return func(_ context.Context, d *data) error {
		d.trace = append(d.trace, "attempted")
		return err
	}
}

// orders exercises every shape a row can have: bare, guarded, with an effect,
// guarded with an effect, a guarded row followed by an unguarded default arm,
// and a self-transition.
var orders = statemachine.MustCompile([]row{
	{From: draft, Event: submit, To: pending, Guard: hasLines},
	{From: draft, Event: cancel, To: cancelled},

	{From: pending, Event: pay, To: paid, Do: record("charge")},
	{From: pending, Event: cancel, To: cancelled},

	{From: paid, Event: ship, To: shipped, Guard: inStock, Do: record("dispatch")},
	{From: paid, Event: ship, To: backorder},
	{From: paid, Event: cancel, To: cancelled, Do: record("refund")},

	// A self-transition: an ordinary row whose To equals its From.
	{From: backorder, Event: ship, To: backorder, Guard: inStock, Do: record("retry")},
})

func ctx() context.Context { return context.Background() }

// --- Fire: the three outcomes ------------------------------------------------

func TestFireSelectsFirstApplicableRow(t *testing.T) {
	for _, tc := range []struct {
		name      string
		from      state
		ev        event
		d         data
		want      state
		wantOK    bool
		wantTrace []string
	}{
		{"bare row", draft, cancel, data{}, cancelled, true, nil},
		{"guard applies", draft, submit, data{lines: 1}, pending, true, nil},
		{"guard declines, no other row", draft, submit, data{}, draft, false, nil},
		{"effect runs", pending, pay, data{}, paid, true, []string{"charge"}},
		{"guarded row wins", paid, ship, data{inStock: true}, shipped, true, []string{"dispatch"}},
		{"default arm wins", paid, ship, data{}, backorder, true, nil},
		{"self-transition", backorder, ship, data{inStock: true}, backorder, true, []string{"retry"}},
		{"wrong state", shipped, pay, data{}, shipped, false, nil},
		{"unknown event", draft, event("teleport"), data{}, draft, false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := tc.d
			got, err := orders.Fire(ctx(), tc.from, tc.ev, &d)
			if got != tc.want {
				t.Errorf("state = %v, want %v", got, tc.want)
			}
			if (err == nil) != tc.wantOK {
				t.Errorf("err = %v, want ok=%v", err, tc.wantOK)
			}
			if !slices.Equal(d.trace, tc.wantTrace) {
				t.Errorf("trace = %v, want %v", d.trace, tc.wantTrace)
			}
		})
	}
}

func TestFireReturnsFromUnchangedOnEveryFailure(t *testing.T) {
	// The assignment idiom is only safe because this holds on all three paths.
	m := statemachine.MustCompile([]row{
		{From: draft, Event: submit, To: pending, Guard: hasLines},
		{From: pending, Event: pay, To: paid, Do: fail(errDeclined)},
	})
	for _, tc := range []struct {
		name string
		from state
		ev   event
	}{
		{"guard declined", draft, submit},
		{"effect failed", pending, pay},
		{"no matching row", shipped, cancel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := data{}
			got, err := m.Fire(ctx(), tc.from, tc.ev, &d)
			if err == nil {
				t.Fatal("want an error")
			}
			if got != tc.from {
				t.Errorf("state = %v, want %v unchanged", got, tc.from)
			}
		})
	}
}

func TestEffectErrorIsReturnedUnwrapped(t *testing.T) {
	m := statemachine.MustCompile([]row{
		{From: pending, Event: pay, To: paid, Do: fail(errDeclined)},
	})
	d := data{}
	got, err := m.Fire(ctx(), pending, pay, &d)

	if !errors.Is(err, errDeclined) {
		t.Errorf("errors.Is(err, errDeclined) = false, err = %v", err)
	}
	if err != errDeclined { //nolint:errorlint // the point of the test is identity
		t.Errorf("err = %#v, want the caller's own error value, unwrapped", err)
	}
	if errors.Is(err, statemachine.ErrNotPermitted) {
		t.Error("an effect failure must not report itself as a refusal")
	}
	if got != pending {
		t.Errorf("state = %v, want pending", got)
	}
	if !slices.Equal(d.trace, []string{"attempted"}) {
		t.Errorf("trace = %v, want the effect to have run once", d.trace)
	}
}

func TestFailingEffectDoesNotFallThroughToTheNextRow(t *testing.T) {
	// switch intuition says the default arm catches this. It does not: the row
	// was already selected.
	m := statemachine.MustCompile([]row{
		{From: paid, Event: ship, To: shipped, Guard: inStock, Do: fail(errDeclined)},
		{From: paid, Event: ship, To: backorder, Do: record("backorder")},
	})
	d := data{inStock: true}
	got, err := m.Fire(ctx(), paid, ship, &d)
	if !errors.Is(err, errDeclined) {
		t.Errorf("err = %v, want the effect's error", err)
	}
	if got != paid {
		t.Errorf("state = %v, want paid", got)
	}
	if slices.Contains(d.trace, "backorder") {
		t.Error("the default arm ran after the selected row's effect failed")
	}
}

func TestGuardsRunInTableOrderAndStopAtTheFirstThatApplies(t *testing.T) {
	var order []string
	guard := func(name string, err error) func(context.Context, *data) error {
		return func(context.Context, *data) error {
			order = append(order, name)
			return err
		}
	}
	m := statemachine.MustCompile([]row{
		{From: draft, Event: submit, To: pending, Guard: guard("first", errNoLines)},
		{From: draft, Event: submit, To: paid, Guard: guard("second", nil)},
		{From: draft, Event: submit, To: shipped, Guard: guard("third", nil)},
	})
	got, err := m.Fire(ctx(), draft, submit, &data{})
	if err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if got != paid {
		t.Errorf("state = %v, want paid (the second row)", got)
	}
	if want := []string{"first", "second"}; !slices.Equal(order, want) {
		t.Errorf("guards called = %v, want %v — the third must not be consulted", order, want)
	}
}

// --- Refusals ----------------------------------------------------------------

func TestRefusalCarriesEveryGuardReason(t *testing.T) {
	m := statemachine.MustCompile([]row{
		{From: paid, Event: ship, To: shipped, Guard: inStock},
		{From: paid, Event: ship, To: backorder, Guard: hasLines},
	})
	_, err := m.Fire(ctx(), paid, ship, &data{})

	if !errors.Is(err, statemachine.ErrNotPermitted) {
		t.Error("want ErrNotPermitted")
	}
	for _, reason := range []error{errOutOfStock, errNoLines} {
		if !errors.Is(err, reason) {
			t.Errorf("errors.Is(err, %v) = false; every reason must survive", reason)
		}
	}
	want := "statemachine: ship in state paid: transition not permitted: out of stock; order has no lines"
	if err.Error() != want {
		t.Errorf("message =\n%q\nwant\n%q", err.Error(), want)
	}
}

func TestRefusalWithoutReasons(t *testing.T) {
	_, err := orders.Fire(ctx(), shipped, pay, &data{})
	want := "statemachine: pay in state shipped: transition not permitted"
	if err.Error() != want {
		t.Errorf("message = %q, want %q", err.Error(), want)
	}
	if !errors.Is(err, statemachine.ErrNotPermitted) {
		t.Error("want ErrNotPermitted")
	}
}

func TestRefusalUnwrapIsStableAndDoesNotRebuild(t *testing.T) {
	// errors.Is walks Unwrap repeatedly; rebuilding the slice there made a
	// refusal allocate on every comparison.
	_, err := orders.Fire(ctx(), draft, submit, &data{})
	u, ok := err.(interface{ Unwrap() []error })
	if !ok {
		t.Fatal("a refusal must implement Unwrap() []error")
	}
	a, b := u.Unwrap(), u.Unwrap()
	if &a[0] != &b[0] {
		t.Error("Unwrap allocated a new slice; it must return the stored one")
	}
	if a[0] != statemachine.ErrNotPermitted {
		t.Errorf("Unwrap()[0] = %v, want ErrNotPermitted first", a[0])
	}
}

func TestTypedNilGuardDeclines(t *testing.T) {
	// Documented hazard: a typed nil in an error interface is not nil, so the
	// row declines and the next one wins.
	type myErr struct{ error }
	m := statemachine.MustCompile([]row{
		{From: draft, Event: submit, To: pending, Guard: func(context.Context, *data) error {
			var e *myErr
			return e
		}},
		{From: draft, Event: submit, To: cancelled},
	})
	got, err := m.Fire(ctx(), draft, submit, &data{})
	if err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if got != cancelled {
		t.Errorf("state = %v, want cancelled — a typed nil declines", got)
	}
}

// --- Compile -----------------------------------------------------------------

func TestCompileRejectsUnreachableRow(t *testing.T) {
	_, err := statemachine.Compile([]row{
		{From: paid, Event: ship, To: backorder},
		{From: paid, Event: ship, To: shipped, Guard: inStock},
	})
	if err == nil {
		t.Fatal("want an error: the guarded row can never be selected")
	}
	want := "statemachine: transitions[1] (paid on ship) is unreachable: transitions[0] has the same From and Event and no Guard"
	if err.Error() != want {
		t.Errorf("message =\n%q\nwant\n%q", err.Error(), want)
	}
}

func TestCompileAcceptsLegalTables(t *testing.T) {
	for _, tc := range []struct {
		name  string
		table []row
	}{
		{"empty", nil},
		{"guarded then default", []row{
			{From: paid, Event: ship, To: shipped, Guard: inStock},
			{From: paid, Event: ship, To: backorder},
		}},
		{"two guarded rows", []row{
			{From: paid, Event: ship, To: shipped, Guard: inStock},
			{From: paid, Event: ship, To: backorder, Guard: hasLines},
		}},
		{"terminal state", []row{{From: draft, Event: cancel, To: cancelled}}},
		{"same From different Event", []row{
			{From: draft, Event: submit, To: pending},
			{From: draft, Event: cancel, To: cancelled},
		}},
		{"same Event different From", []row{
			{From: draft, Event: cancel, To: cancelled},
			{From: paid, Event: cancel, To: cancelled},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := statemachine.Compile(tc.table); err != nil {
				t.Errorf("Compile: %v", err)
			}
		})
	}
}

func TestCompileCopiesTheTable(t *testing.T) {
	table := []row{{From: draft, Event: submit, To: pending}}
	m := statemachine.MustCompile(table)

	table[0].To = cancelled                                         // mutate
	table = append(table, row{From: pending, Event: pay, To: paid}) // and extend

	got, err := m.Fire(ctx(), draft, submit, &data{})
	if err != nil || got != pending {
		t.Errorf("Fire = %v, %v; want pending — the Machine must be immutable", got, err)
	}
	if _, err := m.Fire(ctx(), pending, pay, &data{}); !errors.Is(err, statemachine.ErrNotPermitted) {
		t.Error("a row appended after Compile must not be in the Machine")
	}
	_ = table
}

func TestMustCompilePanicsWithCompileError(t *testing.T) {
	bad := []row{
		{From: paid, Event: ship, To: backorder},
		{From: paid, Event: ship, To: shipped, Guard: inStock},
	}
	_, want := statemachine.Compile(bad)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("MustCompile must panic on an invalid table")
		}
		err, ok := r.(error)
		if !ok || err.Error() != want.Error() {
			t.Errorf("panicked with %v, want the Compile error %v", r, want)
		}
	}()
	statemachine.MustCompile(bad)
}

func TestZeroMachineRefusesEverything(t *testing.T) {
	var m statemachine.Machine[state, event, *data]
	got, err := m.Fire(ctx(), draft, submit, &data{})
	if got != draft || !errors.Is(err, statemachine.ErrNotPermitted) {
		t.Errorf("Fire = %v, %v; want (draft, ErrNotPermitted)", got, err)
	}
	if n := len(maps.Collect(m.Permitted(ctx(), draft, &data{}))); n != 0 {
		t.Errorf("Permitted yielded %d events, want 0", n)
	}
}

func TestCompileOfNilSliceBehavesLikeTheZeroMachine(t *testing.T) {
	var empty []row
	m, err := statemachine.Compile(empty)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if _, err := m.Fire(ctx(), draft, submit, &data{}); !errors.Is(err, statemachine.ErrNotPermitted) {
		t.Errorf("err = %v, want ErrNotPermitted", err)
	}
}

// --- Permitted ---------------------------------------------------------------

func TestPermittedMatchesFire(t *testing.T) {
	// The contract that makes Permitted worth its place: the state it yields is
	// the state Fire would report, for every event, in every state.
	for _, from := range []state{draft, pending, paid, shipped, backorder, cancelled} {
		for _, inStockNow := range []bool{true, false} {
			d := data{lines: 1, inStock: inStockNow}
			offered := maps.Collect(orders.Permitted(ctx(), from, &d))

			for _, ev := range []event{submit, pay, ship, cancel} {
				fresh := data{lines: 1, inStock: inStockNow}
				got, err := orders.Fire(ctx(), from, ev, &fresh)

				want, isOffered := offered[ev]
				switch {
				case isOffered && err != nil:
					t.Errorf("%v/%v: offered but Fire refused: %v", from, ev, err)
				case isOffered && got != want:
					t.Errorf("%v/%v: offered %v but Fire reached %v", from, ev, want, got)
				case !isOffered && err == nil:
					t.Errorf("%v/%v: not offered but Fire succeeded to %v", from, ev, got)
				}
			}
		}
	}
}

func TestPermittedYieldsEachEventOnceInFirstMentionOrder(t *testing.T) {
	d := data{inStock: true}
	var got []event
	for ev := range orders.Permitted(ctx(), paid, &d) {
		got = append(got, ev)
	}
	// paid has three rows: ship (guarded), ship (default), cancel.
	if want := []event{ship, cancel}; !slices.Equal(got, want) {
		t.Errorf("events = %v, want %v — ship must appear once", got, want)
	}
}

func TestPermittedFollowsGuardsToTheRightDestination(t *testing.T) {
	for _, tc := range []struct {
		inStock bool
		want    state
	}{
		{true, shipped},
		{false, backorder},
	} {
		d := data{inStock: tc.inStock}
		got := maps.Collect(orders.Permitted(ctx(), paid, &d))
		if got[ship] != tc.want {
			t.Errorf("inStock=%v: ship leads to %v, want %v", tc.inStock, got[ship], tc.want)
		}
	}
}

func TestPermittedOmitsEventsWhoseEveryRowDeclines(t *testing.T) {
	m := statemachine.MustCompile([]row{
		{From: paid, Event: ship, To: shipped, Guard: inStock},
		{From: paid, Event: cancel, To: cancelled},
	})
	got := maps.Collect(m.Permitted(ctx(), paid, &data{}))
	if _, ok := got[ship]; ok {
		t.Error("ship must not be offered when its only guard declines")
	}
	if _, ok := got[cancel]; !ok {
		t.Error("cancel must still be offered")
	}
}

func TestPermittedRunsNoEffects(t *testing.T) {
	d := data{inStock: true}
	for range orders.Permitted(ctx(), paid, &d) {
	}
	if len(d.trace) != 0 {
		t.Errorf("effects ran during Permitted: %v", d.trace)
	}
}

func TestPermittedIsLazyAndStopsEarly(t *testing.T) {
	var calls int
	counting := func(context.Context, *data) error {
		calls++
		return nil
	}
	m := statemachine.MustCompile([]row{
		{From: draft, Event: submit, To: pending, Guard: counting},
		{From: draft, Event: cancel, To: cancelled, Guard: counting},
		{From: draft, Event: pay, To: paid, Guard: counting},
	})

	seq := m.Permitted(ctx(), draft, &data{})
	if calls != 0 {
		t.Errorf("%d guards ran before ranging; the iterator must be lazy", calls)
	}
	for range seq {
		break
	}
	if calls != 1 {
		t.Errorf("%d guards ran, want 1 — breaking must leave the rest uncalled", calls)
	}
}

func TestPermittedRangesWithOneVariableAndNone(t *testing.T) {
	// iter.Seq2 supports both; the docs use each.
	for range orders.Permitted(ctx(), paid, &data{}) {
	}
	for ev := range orders.Permitted(ctx(), paid, &data{}) {
		_ = ev
	}
}

func TestPermittedWorksWithIterPull2(t *testing.T) {
	next, stop := iter.Pull2(orders.Permitted(ctx(), paid, &data{inStock: true}))
	defer stop()
	ev, to, ok := next()
	if !ok || ev != ship || to != shipped {
		t.Errorf("first = (%v, %v, %v), want (ship, shipped, true)", ev, to, ok)
	}
}

func TestPermittedDoesNotLeakGoroutinesWhenFullyDrained(t *testing.T) {
	before := runtime.NumGoroutine()
	for range 100 {
		next, stop := iter.Pull2(orders.Permitted(ctx(), paid, &data{}))
		next()
		stop()
	}
	runtime.GC()
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Errorf("goroutines %d -> %d", before, after)
	}
}

// --- Concurrency -------------------------------------------------------------

func TestMachineIsSafeForConcurrentUse(t *testing.T) {
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			for range 200 {
				d := data{lines: 1, inStock: true}
				if _, err := orders.Fire(ctx(), paid, ship, &d); err != nil {
					t.Error(err)
					return
				}
				for range orders.Permitted(ctx(), paid, &d) {
				}
			}
		})
	}
	wg.Wait()
}

func TestFireIsReentrant(t *testing.T) {
	// No lock and no in-flight state, so an effect may fire another machine.
	inner := statemachine.MustCompile([]row{
		{From: draft, Event: cancel, To: cancelled, Do: record("inner")},
	})
	outer := statemachine.MustCompile([]row{
		{From: pending, Event: pay, To: paid, Do: func(c context.Context, d *data) error {
			_, err := inner.Fire(c, draft, cancel, d)
			return err
		}},
	})
	d := data{}
	got, err := outer.Fire(ctx(), pending, pay, &d)
	if err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if got != paid {
		t.Errorf("state = %v, want paid — the outer row's To, not the nested one's", got)
	}
	if !slices.Equal(d.trace, []string{"inner"}) {
		t.Errorf("trace = %v, want the nested effect to have run", d.trace)
	}
}

// --- Documented invariants ---------------------------------------------------

func TestFireIgnoresContextCancellation(t *testing.T) {
	// Fire never reads ctx; only Guard and Do may honor it.
	dead, stop := context.WithCancel(ctx())
	stop()
	got, err := orders.Fire(dead, draft, cancel, &data{})
	if err != nil {
		t.Errorf("err = %v; Fire must not check ctx itself", err)
	}
	if got != cancelled {
		t.Errorf("state = %v, want cancelled", got)
	}
}

func TestContextReachesGuardsAndEffects(t *testing.T) {
	type ckey struct{}
	var seen []string
	probe := func(name string) func(context.Context, *data) error {
		return func(c context.Context, _ *data) error {
			if v, _ := c.Value(ckey{}).(string); v == "carried" {
				seen = append(seen, name)
			}
			return nil
		}
	}
	m := statemachine.MustCompile([]row{
		{From: draft, Event: submit, To: pending, Guard: probe("guard"), Do: probe("do")},
	})
	c := context.WithValue(ctx(), ckey{}, "carried")
	if _, err := m.Fire(c, draft, submit, &data{}); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if want := []string{"guard", "do"}; !slices.Equal(seen, want) {
		t.Errorf("ctx reached %v, want %v", seen, want)
	}
}

func TestNilGuardAndNilEffectAreNoOps(t *testing.T) {
	m := statemachine.MustCompile([]row{{From: draft, Event: submit, To: pending}})
	got, err := m.Fire(ctx(), draft, submit, &data{})
	if got != pending || err != nil {
		t.Errorf("Fire = %v, %v; want (pending, nil)", got, err)
	}
}

func TestSelfTransitionRunsItsEffectOnce(t *testing.T) {
	m := statemachine.MustCompile([]row{
		{From: paid, Event: ship, To: paid, Do: record("retry")},
	})
	d := data{}
	got, err := m.Fire(ctx(), paid, ship, &d)
	if got != paid || err != nil {
		t.Fatalf("Fire = %v, %v", got, err)
	}
	if !slices.Equal(d.trace, []string{"retry"}) {
		t.Errorf("trace = %v, want exactly one run", d.trace)
	}
}

func TestUnknownStateHasNoAffordances(t *testing.T) {
	// A value loaded from storage that no row mentions: refuses, never panics.
	corrupt := state("who-knows")
	got, err := orders.Fire(ctx(), corrupt, ship, &data{})
	if got != corrupt || !errors.Is(err, statemachine.ErrNotPermitted) {
		t.Errorf("Fire = %v, %v; want (%v, ErrNotPermitted)", got, err, corrupt)
	}
	if n := len(maps.Collect(orders.Permitted(ctx(), corrupt, &data{}))); n != 0 {
		t.Errorf("Permitted yielded %d events for an unknown state", n)
	}
}

func TestRefusalMessageNamesTheEventAndState(t *testing.T) {
	_, err := orders.Fire(ctx(), shipped, ship, &data{})
	for _, want := range []string{"statemachine:", string(ship), string(shipped)} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("message %q does not mention %q", err, want)
		}
	}
}

// --- Benchmarks --------------------------------------------------------------

// benchMachine has no recording effects, so the benchmarks below measure Fire
// rather than a test fixture's growing trace slice.
var benchMachine = statemachine.MustCompile([]row{
	{From: paid, Event: ship, To: shipped, Guard: inStock},
	{From: paid, Event: ship, To: backorder},
	{From: paid, Event: cancel, To: cancelled},
})

// The accepted path: a map lookup, one guard, and a return.
func BenchmarkFireAccepted(b *testing.B) {
	d := data{lines: 1, inStock: true}
	b.ReportAllocs()
	for b.Loop() {
		benchMachine.Fire(ctx(), paid, ship, &d)
	}
}

// The default arm: the first guard declines, so a reason is collected and
// discarded before the second row wins.
func BenchmarkFireDefaultArm(b *testing.B) {
	d := data{}
	b.ReportAllocs()
	for b.Loop() {
		benchMachine.Fire(ctx(), paid, ship, &d)
	}
}

// The refused path, which the docs steer callers toward instead of
// check-then-act: it must not be expensive.
func BenchmarkFireRefused(b *testing.B) {
	d := data{}
	b.ReportAllocs()
	for b.Loop() {
		benchMachine.Fire(ctx(), shipped, pay, &d)
	}
}

func BenchmarkRefusalErrorsIs(b *testing.B) {
	_, err := orders.Fire(ctx(), draft, submit, &data{})
	b.ReportAllocs()
	for b.Loop() {
		if !errors.Is(err, statemachine.ErrNotPermitted) {
			b.Fatal("want a refusal")
		}
	}
}

func BenchmarkPermitted(b *testing.B) {
	d := data{inStock: true}
	b.ReportAllocs()
	for b.Loop() {
		for range benchMachine.Permitted(ctx(), paid, &d) {
		}
	}
}

func BenchmarkCompile(b *testing.B) {
	table := []row{
		{From: draft, Event: submit, To: pending, Guard: hasLines},
		{From: draft, Event: cancel, To: cancelled},
		{From: pending, Event: pay, To: paid},
		{From: paid, Event: ship, To: shipped, Guard: inStock},
		{From: paid, Event: ship, To: backorder},
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := statemachine.Compile(table); err != nil {
			b.Fatal(err)
		}
	}
}
