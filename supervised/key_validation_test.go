package supervised

import (
	"context"
	"errors"
	"math"
	"testing"
)

func numericDefinition() Definition[float64, float64, struct{}] {
	check := func(context.Context, Change[float64, float64], struct{}) error { return nil }
	return Definition[float64, float64, struct{}]{
		ID: "numbers/v1", Initial: 0,
		States:         []State[float64, float64]{{Name: 0}, {Name: 1, Terminal: true}},
		Events:         []float64{1},
		Transitions:    []Transition[float64, float64, struct{}]{{ID: "advance", From: 0, Event: 1, To: 1}},
		Preconditions:  []Precondition[float64, float64, struct{}]{func(context.Context, Attempt[float64, float64], struct{}) error { return nil }},
		Invariants:     []Check[float64, float64, struct{}]{check},
		Postconditions: []Check[float64, float64, struct{}]{check},
		Reconcile:      []Reconciler[float64, float64, struct{}]{func(context.Context, Snapshot[float64, float64], struct{}) error { return nil }},
	}
}

func TestSupervisorRejectsNaNBeforeMapUse(t *testing.T) {
	nan := math.NaN()
	mutations := []func(*Definition[float64, float64, struct{}]){
		func(d *Definition[float64, float64, struct{}]) { d.Initial = nan },
		func(d *Definition[float64, float64, struct{}]) { d.States[0].Name = nan },
		func(d *Definition[float64, float64, struct{}]) { d.States[0].Refuse = []float64{nan} },
		func(d *Definition[float64, float64, struct{}]) { d.Events[0] = nan },
		func(d *Definition[float64, float64, struct{}]) { d.Transitions[0].From = nan },
		func(d *Definition[float64, float64, struct{}]) { d.Transitions[0].Event = nan },
		func(d *Definition[float64, float64, struct{}]) { d.Transitions[0].To = nan },
	}
	for i, mutate := range mutations {
		d := numericDefinition()
		mutate(&d)
		if _, err := Compile(d); !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("mutation %d: %v", i, err)
		}
	}
	machine := MustCompile(numericDefinition())
	run, err := New(machine, limits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = run.Start(context.Background(), struct{}{}); err != nil {
		t.Fatal(err)
	}
	before := run.Snapshot()
	if _, err = run.Issue(context.Background(), nan, struct{}{}); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("Issue NaN: %v", err)
	}
	if run.Snapshot().Attempt != before.Attempt {
		t.Fatal("invalid event allocated an Attempt")
	}
	invalid := before
	invalid.State = nan
	if _, err = Restore(machine, invalid, limits()); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("Restore NaN: %v", err)
	}
	_ = run.Trip(nil)
	if _, err = run.Adjudicate(context.Background(), Decision[float64]{Outcome: AdjudicateOverride, State: nan}, struct{}{}); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("Override NaN: %v", err)
	}
}

func TestSnapshotRequiresIncarnationIdentity(t *testing.T) {
	machine := MustCompile(numericDefinition())
	run, err := New(machine, limits())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := run.Snapshot()
	snapshot.IncarnationID = ""
	if _, err = Restore(machine, snapshot, limits()); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("missing incarnation: %v", err)
	}
}

func TestRestoreAdoptionPreservesOriginalChange(t *testing.T) {
	clock := newFakeClock()
	definition := validDefinition()
	var original Change[testState, testEvent]
	definition.Transitions[0].Issue = func(_ context.Context, change Change[testState, testEvent], _ *testData) error {
		original = change
		return nil
	}
	machine := MustCompile(definition)
	run, err := newUnjournaledWithClock(machine, limits(), clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = run.Start(context.Background(), &testData{}); err != nil {
		t.Fatal(err)
	}
	if _, err = run.Issue(context.Background(), testStart, &testData{}); err != nil {
		t.Fatal(err)
	}
	durable := run.Snapshot()
	if durable.Pending == nil || durable.Pending.StartedAt != original.StartedAt {
		t.Fatalf("pending original timing: %+v", durable.Pending)
	}
	restored, err := RestoreWithOptions(machine, durable, Options[testState, testEvent]{Limits: limits(), Unjournaled: true, Clock: newFakeClock()})
	if err != nil {
		t.Fatal(err)
	}
	if restored.Status().Fault.Change != original {
		t.Fatal("restored fault lost original Change")
	}
	adopted, err := restored.Adjudicate(context.Background(), Decision[testState]{Outcome: AdjudicateAdopt}, &testData{})
	if err != nil {
		t.Fatal(err)
	}
	records := restored.Records()
	record := records[len(records)-1]
	if adopted.Change != original || record.Change != original || record.Attempt != original.Identifier() || record.Revision != original.Revision+1 || record.IncarnationID == original.IncarnationID {
		t.Fatalf("adoption evidence mismatch: %+v", record)
	}
}
