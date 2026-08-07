package supervised

import (
	"context"
	"strings"
	"testing"
)

type testState string
type testEvent string

const (
	testIdle    testState = "idle"
	testRunning testState = "running"
	testOther   testState = "other"

	testStart testEvent = "start"
	testStop  testEvent = "stop"
)

type testData struct {
	trace    []string
	allow    bool
	verified bool
}

func passPrecondition(context.Context, Attempt[testState, testEvent], *testData) error {
	return nil
}

func passCheck(context.Context, Change[testState, testEvent], *testData) error {
	return nil
}

func passReconcile(context.Context, Snapshot[testState], *testData) error {
	return nil
}

func passAction(context.Context, Change[testState, testEvent], *testData) error {
	return nil
}

func validDefinition() Definition[testState, testEvent, *testData] {
	return Definition[testState, testEvent, *testData]{
		ID:      "test-machine/v1",
		Initial: testIdle,
		States: []State[testState, testEvent]{
			{Name: testIdle},
			{Name: testRunning, Terminal: true},
		},
		Events: []testEvent{testStart},
		Transitions: []Transition[testState, testEvent, *testData]{
			{
				ID: "start-running", From: testIdle, Event: testStart, To: testRunning,
				Issue: passAction, Verify: passCheck,
			},
		},
		Preconditions:  []Precondition[testState, testEvent, *testData]{passPrecondition},
		Invariants:     []Check[testState, testEvent, *testData]{passCheck},
		Postconditions: []Check[testState, testEvent, *testData]{passCheck},
		Reconcile:      []Reconciler[testState, *testData]{passReconcile},
	}
}

func TestCompileStrictDefinitionAndInspection(t *testing.T) {
	definition := validDefinition()
	machine, err := Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	if machine.ID() != "test-machine/v1" {
		t.Fatalf("ID = %q", machine.ID())
	}
	if initial, ok := machine.Initial(); !ok || initial != testIdle {
		t.Fatalf("Initial = %v, %v", initial, ok)
	}

	var states []StateInfo[testState]
	for state := range machine.States() {
		states = append(states, state)
	}
	if len(states) != 2 || states[0].Name != testIdle || states[1].Name != testRunning || !states[1].Terminal {
		t.Fatalf("States = %+v", states)
	}
	var events []testEvent
	for event := range machine.Events() {
		events = append(events, event)
	}
	if len(events) != 1 || events[0] != testStart {
		t.Fatalf("Events = %v", events)
	}
	var transitions []TransitionInfo[testState, testEvent]
	for transition := range machine.Transitions() {
		transitions = append(transitions, transition)
	}
	if len(transitions) != 1 || transitions[0].ID != "start-running" || !transitions[0].Issues {
		t.Fatalf("Transitions = %+v", transitions)
	}
	if !machine.Refused(testRunning, testStart) || machine.Refused(testIdle, testStart) {
		t.Fatalf("Refused terminal/idle = %v/%v", machine.Refused(testRunning, testStart), machine.Refused(testIdle, testStart))
	}
}

func TestCompileCopiesDefinitionSlices(t *testing.T) {
	definition := validDefinition()
	machine := MustCompile(definition)

	definition.States[0].Name = testOther
	definition.Events[0] = testStop
	definition.Transitions[0].ID = "mutated"
	definition.Preconditions[0] = nil
	definition.Invariants[0] = nil
	definition.Postconditions[0] = nil
	definition.Reconcile[0] = nil

	initial, _ := machine.Initial()
	if initial != testIdle {
		t.Fatalf("Initial = %v", initial)
	}
	for transition := range machine.Transitions() {
		if transition.ID != "start-running" {
			t.Fatalf("Transition ID = %q", transition.ID)
		}
	}
	supervisor, err := New(machine, Limits{OperationTimeout: 1, VerificationTimeout: 1})
	if err != nil || supervisor == nil {
		t.Fatalf("New = %v, %v", supervisor, err)
	}
}

func TestCompileReportsStrictDefinitionProblems(t *testing.T) {
	noops := validDefinition()
	noops.ID = ""
	noops.States = append(noops.States,
		State[testState, testEvent]{Name: testIdle},
		State[testState, testEvent]{Name: testOther},
	)
	noops.Events = append(noops.Events, testStart, testStop)
	noops.Initial = testState("missing")
	noops.Preconditions = nil
	noops.Invariants = []Check[testState, testEvent, *testData]{nil}
	noops.Postconditions = nil
	noops.Reconcile = []Reconciler[testState, *testData]{nil}
	noops.Transitions = append(noops.Transitions,
		Transition[testState, testEvent, *testData]{
			ID: "start-running", From: testRunning, Event: testStart, To: testIdle,
			Issue: passAction,
		},
	)

	_, err := Compile(noops)
	if err == nil {
		t.Fatal("Compile succeeded")
	}
	for _, want := range []string{
		"definition ID is empty",
		"duplicates state",
		"duplicates event",
		"initial state missing is undeclared",
		"definition has no preconditions",
		"invariants[0] is nil",
		"definition has no postconditions",
		"reconcile[0] is nil",
		"duplicates ID",
		"both Issue and Verify or neither",
		"leaves terminal state",
		"neither handles nor refuses event stop",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not contain %q:\n%v", want, err)
		}
	}
}

func TestCompileRejectsUnreachableState(t *testing.T) {
	definition := validDefinition()
	definition.States = append(definition.States, State[testState, testEvent]{Name: testOther, Terminal: true})
	_, err := Compile(definition)
	if err == nil || !strings.Contains(err.Error(), "state other is unreachable") {
		t.Fatalf("Compile error = %v", err)
	}
}

func TestCompileRequiresNonTerminalStateToMakeProgress(t *testing.T) {
	definition := validDefinition()
	definition.States = []State[testState, testEvent]{{Name: testIdle, Refuse: []testEvent{testStart}}}
	definition.Transitions = nil
	_, err := Compile(definition)
	if err == nil || !strings.Contains(err.Error(), "non-terminal state idle has no outgoing transition") {
		t.Fatalf("Compile error = %v", err)
	}
}

func TestCompileRejectsHiddenRowsAndExplicitRefusalConflict(t *testing.T) {
	definition := validDefinition()
	definition.States[0].Refuse = []testEvent{testStop}
	definition.Events = append(definition.Events, testStop)
	definition.Transitions = append(definition.Transitions,
		Transition[testState, testEvent, *testData]{
			ID: "hidden", From: testIdle, Event: testStart, To: testRunning,
			Issue: passAction, Verify: passCheck,
		},
		Transition[testState, testEvent, *testData]{
			ID: "refused", From: testIdle, Event: testStop, To: testRunning,
		},
	)
	_, err := Compile(definition)
	if err == nil || !strings.Contains(err.Error(), "is unreachable") || !strings.Contains(err.Error(), "explicitly refused") {
		t.Fatalf("Compile error = %v", err)
	}
}

func TestCompileRejectsInterfaceStateAndEventTypes(t *testing.T) {
	definition := Definition[any, any, struct{}]{
		ID:             "interfaces/v1",
		States:         []State[any, any]{{Name: "idle", Terminal: true}},
		Events:         []any{"start"},
		Initial:        "idle",
		Preconditions:  []Precondition[any, any, struct{}]{func(context.Context, Attempt[any, any], struct{}) error { return nil }},
		Invariants:     []Check[any, any, struct{}]{func(context.Context, Change[any, any], struct{}) error { return nil }},
		Postconditions: []Check[any, any, struct{}]{func(context.Context, Change[any, any], struct{}) error { return nil }},
		Reconcile:      []Reconciler[any, struct{}]{func(context.Context, Snapshot[any], struct{}) error { return nil }},
	}
	_, err := Compile(definition)
	if err == nil || !strings.Contains(err.Error(), "state type must not be an interface") || !strings.Contains(err.Error(), "event type must not be an interface") {
		t.Fatalf("Compile error = %v", err)
	}
}

func TestCompileReportsEmptyAndMalformedDeclarations(t *testing.T) {
	empty := Definition[testState, testEvent, *testData]{ID: "empty/v1"}
	_, err := Compile(empty)
	if err == nil || !strings.Contains(err.Error(), "no states") || !strings.Contains(err.Error(), "no events") {
		t.Fatalf("empty Compile error = %v", err)
	}

	definition := validDefinition()
	definition.Preconditions = append(definition.Preconditions, nil)
	definition.Postconditions = append(definition.Postconditions, nil)
	definition.States[0].Refuse = []testEvent{testStop, testStop, testEvent("unknown")}
	definition.Events = append(definition.Events, testStop)
	definition.Transitions = append(definition.Transitions,
		Transition[testState, testEvent, *testData]{
			From: testState("unknown"), Event: testEvent("unknown"), To: testState("missing"),
			Preconditions: []Check[testState, testEvent, *testData]{nil},
		},
	)
	_, err = Compile(definition)
	if err == nil {
		t.Fatal("malformed Compile succeeded")
	}
	for _, want := range []string{
		"preconditions[1] is nil",
		"postconditions[1] is nil",
		"duplicates event stop",
		"names undeclared event unknown",
		"has an empty ID",
		"From names undeclared value unknown",
		"To names undeclared value missing",
		"Event names undeclared value unknown",
		"Preconditions[0] is nil",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not contain %q:\n%v", want, err)
		}
	}
}

func TestMustCompilePanicsAndNilMachineInspectionIsEmpty(t *testing.T) {
	func() {
		defer func() {
			if recover() == nil {
				t.Error("MustCompile did not panic")
			}
		}()
		MustCompile(Definition[testState, testEvent, *testData]{})
	}()

	var machine *Machine[testState, testEvent, *testData]
	if machine.ID() != "" {
		t.Fatalf("nil ID = %q", machine.ID())
	}
	if initial, ok := machine.Initial(); ok || initial != "" {
		t.Fatalf("nil Initial = %v, %v", initial, ok)
	}
	for range machine.States() {
		t.Fatal("nil States yielded")
	}
	for range machine.Events() {
		t.Fatal("nil Events yielded")
	}
	for range machine.Transitions() {
		t.Fatal("nil Transitions yielded")
	}
	if machine.Refused(testIdle, testStart) {
		t.Fatal("nil Machine refused a value")
	}

	machine = MustCompile(validDefinition())
	for range machine.States() {
		break
	}
	for range machine.Events() {
		break
	}
	for range machine.Transitions() {
		break
	}
	if machine.Refused(testOther, testStart) || machine.Refused(testIdle, testStop) {
		t.Fatal("unknown state or event reported refused")
	}
}
