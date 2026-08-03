package statechart_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/open-ships/statemachine/statechart"
)

type testState string
type testEvent string

const (
	root  testState = "root"
	a     testState = "a"
	a1    testState = "a1"
	a2    testState = "a2"
	b     testState = "b"
	b1    testState = "b1"
	other testState = "other"
)

const (
	goB          testEvent = "go-b"
	reset        testEvent = "reset"
	touch        testEvent = "touch"
	fallback     testEvent = "fallback"
	unknownEvent testEvent = "unknown"
)

type testData struct {
	allow bool
	trace []string
}

type action = statechart.Action[testState, testEvent, *testData]
type state = statechart.State[testState, testEvent, *testData]
type transition = statechart.Transition[testState, testEvent, *testData]
type definition = statechart.Definition[testState, testEvent, *testData]

func record(label string) action {
	return func(_ context.Context, _ statechart.Info[testState, testEvent], data *testData) error {
		data.trace = append(data.trace, label)
		return nil
	}
}

func baseStates() []state {
	return []state{
		{Name: root}, {Name: a}, {Name: a1}, {Name: a2},
		{Name: b}, {Name: b1}, {Name: other},
	}
}

func hierarchy() []statechart.Substate[testState] {
	return []statechart.Substate[testState]{
		{Child: a, Parent: root},
		{Child: a1, Parent: a},
		{Child: a2, Parent: a},
		{Child: b, Parent: root},
		{Child: b1, Parent: b},
	}
}

func TestNewResolvesInitialChainWithoutEntryActions(t *testing.T) {
	entered := 0
	states := baseStates()
	states[1].Entry = []action{func(context.Context, statechart.Info[testState, testEvent], *testData) error {
		entered++
		return nil
	}}
	chart := statechart.MustCompile(definition{
		States:    states,
		Substates: hierarchy(),
		Initials: []statechart.Initial[testState]{
			{Parent: root, Child: a},
			{Parent: a, Child: a1},
			{Parent: b, Child: b1},
		},
	})
	instance, err := chart.New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := instance.State(); got != a1 {
		t.Fatalf("State = %v, want %v", got, a1)
	}
	if entered != 0 {
		t.Fatalf("New ran %d entry actions; want none", entered)
	}
	if _, err := chart.New(testState("missing")); err == nil {
		t.Fatal("New accepted an undeclared state")
	}
	var nilChart *statechart.Chart[testState, testEvent, *testData]
	if _, err := nilChart.New(root); err == nil {
		t.Fatal("nil Chart.New succeeded")
	}
}

func TestZeroInstanceRefusesWithoutPanicking(t *testing.T) {
	var instance statechart.Instance[testState, testEvent, *testData]
	if got := instance.State(); got != "" {
		t.Fatalf("zero State = %q", got)
	}
	if err := instance.Fire(context.Background(), goB, &testData{}); !errors.Is(err, statechart.ErrNotPermitted) {
		t.Fatalf("zero Fire = %v, want ErrNotPermitted", err)
	}
	var count int
	for range instance.Permitted(context.Background(), &testData{}) {
		count++
	}
	if count != 0 {
		t.Fatalf("zero Permitted yielded %d events", count)
	}
}

func TestCompositeWithoutInitialMayRemainActive(t *testing.T) {
	chart := statechart.MustCompile(definition{
		States:    []state{{Name: a}, {Name: a1}},
		Substates: []statechart.Substate[testState]{{Child: a1, Parent: a}},
	})
	instance, err := chart.New(a)
	if err != nil {
		t.Fatal(err)
	}
	if got := instance.State(); got != a {
		t.Fatalf("State = %v, want active composite %v", got, a)
	}
}

func TestCompileAggregatesDefinitionDefects(t *testing.T) {
	_, err := statechart.Compile(definition{
		States: []state{{Name: root}, {Name: a}, {Name: b}, {Name: root}},
		Substates: []statechart.Substate[testState]{
			{Child: a, Parent: b},
			{Child: b, Parent: a},
			{Child: a, Parent: root},
			{Child: testState("missing"), Parent: root},
		},
		Initials: []statechart.Initial[testState]{
			{Parent: root, Child: a},
			{Parent: root, Child: b},
		},
		Transitions: []transition{
			{From: root, Event: goB, To: root, Kind: statechart.External},
			{From: a, Event: touch, To: b, Kind: statechart.Internal},
			{From: b, Event: reset, To: a, Kind: statechart.Reentry},
			{From: root, Event: fallback, To: a},
			{From: root, Event: fallback, To: b, Guard: func(context.Context, statechart.Info[testState, testEvent], *testData) error { return nil }},
			{From: testState("missing"), Event: goB, To: root},
		},
	})
	if err == nil {
		t.Fatal("Compile accepted invalid definition")
	}
	message := err.Error()
	for _, fragment := range []string{
		"duplicates state", "second parent", "undeclared state", "hierarchy contains a cycle",
		"second initial child", "not an immediate child", "is external", "is internal",
		"is reentry", "is unreachable",
	} {
		if !strings.Contains(message, fragment) {
			t.Errorf("Compile error does not contain %q:\n%s", fragment, message)
		}
	}
}

func TestCompileRejectsInvalidKindAndSelfParent(t *testing.T) {
	_, err := statechart.Compile(definition{
		States:      []state{{Name: a}, {Name: b}},
		Substates:   []statechart.Substate[testState]{{Child: a, Parent: a}},
		Transitions: []transition{{From: a, Event: goB, To: b, Kind: statechart.Kind(99)}},
	})
	if err == nil || !strings.Contains(err.Error(), "own parent") || !strings.Contains(err.Error(), "invalid kind") {
		t.Fatalf("Compile error = %v", err)
	}
}

func TestMustCompilePanicsWithCompileError(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("MustCompile did not panic")
		}
	}()
	statechart.MustCompile(definition{States: []state{{Name: a}, {Name: a}}})
}

func TestDefinitionIsCopied(t *testing.T) {
	data := &testData{}
	states := []state{{Name: a, Exit: []action{record("original exit")}}, {Name: b, Entry: []action{record("original entry")}}}
	transitions := []transition{{From: a, Event: goB, To: b, Do: record("original effect")}}
	chart := statechart.MustCompile(definition{States: states, Transitions: transitions})

	states[0].Exit[0] = record("mutated exit")
	states[1].Entry[0] = record("mutated entry")
	transitions[0].Do = record("mutated effect")
	instance, _ := chart.New(a)
	if err := instance.Fire(context.Background(), goB, data); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if want := []string{"original exit", "original effect", "original entry"}; !slices.Equal(data.trace, want) {
		t.Fatalf("trace = %v, want %v", data.trace, want)
	}
}

func TestInheritedLookupFallsBackThroughDeclinedGuards(t *testing.T) {
	errLeaf := errors.New("leaf closed")
	errParent := errors.New("parent closed")
	var guardInfos []statechart.Info[testState, testEvent]
	chart := statechart.MustCompile(definition{
		States: []state{{Name: root}, {Name: a}, {Name: a1}, {Name: b}},
		Substates: []statechart.Substate[testState]{
			{Child: a, Parent: root}, {Child: a1, Parent: a}, {Child: b, Parent: root},
		},
		Transitions: []transition{
			{From: a1, Event: goB, To: b, Guard: func(_ context.Context, info statechart.Info[testState, testEvent], _ *testData) error {
				guardInfos = append(guardInfos, info)
				return errLeaf
			}},
			{From: a, Event: goB, To: b, Guard: func(_ context.Context, info statechart.Info[testState, testEvent], _ *testData) error {
				guardInfos = append(guardInfos, info)
				return errParent
			}},
			{From: root, Event: goB, To: b, Do: record("root handled")},
		},
	})
	instance, _ := chart.New(a1)
	data := &testData{}
	if err := instance.Fire(context.Background(), goB, data); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if instance.State() != b || !slices.Equal(data.trace, []string{"root handled"}) {
		t.Fatalf("State/trace = %v/%v", instance.State(), data.trace)
	}
	if len(guardInfos) != 2 || guardInfos[0].Source != a1 || guardInfos[0].Handler != a1 ||
		guardInfos[1].Source != a1 || guardInfos[1].Handler != a {
		t.Fatalf("guard Info = %+v", guardInfos)
	}

	refusing := statechart.MustCompile(definition{
		States: []state{{Name: root}, {Name: a}, {Name: a1}, {Name: b}},
		Substates: []statechart.Substate[testState]{
			{Child: a, Parent: root}, {Child: a1, Parent: a}, {Child: b, Parent: root},
		},
		Transitions: []transition{
			{From: a1, Event: goB, To: b, Guard: func(context.Context, statechart.Info[testState, testEvent], *testData) error { return errLeaf }},
			{From: a, Event: goB, To: b, Guard: func(context.Context, statechart.Info[testState, testEvent], *testData) error { return errParent }},
		},
	})
	instance, _ = refusing.New(a1)
	err := instance.Fire(context.Background(), goB, data)
	for _, want := range []error{statechart.ErrNotPermitted, errLeaf, errParent} {
		if !errors.Is(err, want) {
			t.Errorf("errors.Is(%v) = false; err = %v", want, err)
		}
	}
	if got := err.Error(); got != "statechart: go-b in state a1: transition not permitted: leaf closed; parent closed" {
		t.Errorf("refusal text = %q", got)
	}
	if instance.State() != a1 {
		t.Fatalf("refusal changed state to %v", instance.State())
	}
}

func TestExternalTransitionOrdersLifecycleAroundCommit(t *testing.T) {
	var instance *statechart.Instance[testState, testEvent, *testData]
	observe := func(label string, want testState) action {
		return func(_ context.Context, info statechart.Info[testState, testEvent], data *testData) error {
			data.trace = append(data.trace, fmt.Sprintf("%s:%s", label, instance.State()))
			if info != (statechart.Info[testState, testEvent]{
				Source: a1, Handler: a, Target: b, Destination: b1, Event: goB, Kind: statechart.External,
			}) {
				t.Errorf("Info = %+v", info)
			}
			if instance.State() != want {
				t.Errorf("%s observed %v, want %v", label, instance.State(), want)
			}
			return nil
		}
	}
	chart := statechart.MustCompile(definition{
		States: []state{
			{Name: root},
			{Name: a, Exit: []action{observe("exit a", a1)}},
			{Name: a1, Exit: []action{observe("exit a1", a1)}},
			{Name: b, Entry: []action{observe("enter b", b1)}},
			{Name: b1, Entry: []action{observe("enter b1", b1)}},
		},
		Substates: []statechart.Substate[testState]{
			{Child: a, Parent: root}, {Child: a1, Parent: a},
			{Child: b, Parent: root}, {Child: b1, Parent: b},
		},
		Initials:    []statechart.Initial[testState]{{Parent: b, Child: b1}},
		Transitions: []transition{{From: a, Event: goB, To: b, Do: observe("effect", a1)}},
	})
	instance, _ = chart.New(a1)
	data := &testData{}
	if err := instance.Fire(context.Background(), goB, data); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	want := []string{"exit a1:a1", "exit a:a1", "effect:a1", "enter b:b1", "enter b1:b1"}
	if !slices.Equal(data.trace, want) {
		t.Fatalf("trace = %v, want %v", data.trace, want)
	}
}

func TestExternalTransitionUsesActiveTargetLCAForAncestorAndDescendant(t *testing.T) {
	t.Run("target ancestor", func(t *testing.T) {
		chart := statechart.MustCompile(definition{
			States: []state{
				{Name: a, Entry: []action{record("enter a")}, Exit: []action{record("exit a")}},
				{Name: a1, Exit: []action{record("exit a1")}},
				{Name: a2, Entry: []action{record("enter a2")}},
			},
			Substates: []statechart.Substate[testState]{{Child: a1, Parent: a}, {Child: a2, Parent: a}},
			Initials:  []statechart.Initial[testState]{{Parent: a, Child: a2}},
			Transitions: []transition{{
				From: a1, Event: reset, To: a, Do: record("effect"),
			}},
		})
		instance, _ := chart.New(a1)
		data := &testData{}
		if err := instance.Fire(context.Background(), reset, data); err != nil {
			t.Fatal(err)
		}
		want := []string{"exit a1", "effect", "enter a2"}
		if instance.State() != a2 || !slices.Equal(data.trace, want) {
			t.Fatalf("State/trace = %v/%v, want %v/%v", instance.State(), data.trace, a2, want)
		}
	})

	t.Run("target descendant", func(t *testing.T) {
		chart := statechart.MustCompile(definition{
			States: []state{
				{Name: a, Entry: []action{record("enter a")}, Exit: []action{record("exit a")}},
				{Name: a1, Entry: []action{record("enter a1")}},
			},
			Substates: []statechart.Substate[testState]{{Child: a1, Parent: a}},
			Transitions: []transition{{
				From: a, Event: goB, To: a1, Do: record("effect"),
			}},
		})
		instance, _ := chart.New(a)
		data := &testData{}
		if err := instance.Fire(context.Background(), goB, data); err != nil {
			t.Fatal(err)
		}
		want := []string{"effect", "enter a1"}
		if instance.State() != a1 || !slices.Equal(data.trace, want) {
			t.Fatalf("State/trace = %v/%v, want %v/%v", instance.State(), data.trace, a1, want)
		}
	})
}

func TestInternalTransitionRunsOnlyEffectAndKeepsLeaf(t *testing.T) {
	chart := statechart.MustCompile(definition{
		States: []state{
			{Name: a, Entry: []action{record("enter a")}, Exit: []action{record("exit a")}},
			{Name: a1, Entry: []action{record("enter a1")}, Exit: []action{record("exit a1")}},
		},
		Substates: []statechart.Substate[testState]{{Child: a1, Parent: a}},
		Transitions: []transition{{From: a, Event: touch, To: a, Kind: statechart.Internal, Do: func(_ context.Context, info statechart.Info[testState, testEvent], data *testData) error {
			if info.Source != a1 || info.Handler != a || info.Target != a || info.Destination != a1 || info.Kind != statechart.Internal {
				t.Errorf("Info = %+v", info)
			}
			data.trace = append(data.trace, "effect")
			return nil
		}}},
	})
	instance, _ := chart.New(a1)
	data := &testData{}
	if err := instance.Fire(context.Background(), touch, data); err != nil {
		t.Fatal(err)
	}
	if instance.State() != a1 || !slices.Equal(data.trace, []string{"effect"}) {
		t.Fatalf("State/trace = %v/%v", instance.State(), data.trace)
	}
}

func TestReentryExitsThroughHandlerAndEntersItsInitialChain(t *testing.T) {
	chart := statechart.MustCompile(definition{
		States: []state{
			{Name: a, Entry: []action{record("enter a")}, Exit: []action{record("exit a")}},
			{Name: a1, Exit: []action{record("exit a1")}},
			{Name: a2, Entry: []action{record("enter a2")}},
		},
		Substates:   []statechart.Substate[testState]{{Child: a1, Parent: a}, {Child: a2, Parent: a}},
		Initials:    []statechart.Initial[testState]{{Parent: a, Child: a2}},
		Transitions: []transition{{From: a, Event: reset, To: a, Kind: statechart.Reentry, Do: record("effect")}},
	})
	instance, _ := chart.New(a1)
	data := &testData{}
	if err := instance.Fire(context.Background(), reset, data); err != nil {
		t.Fatal(err)
	}
	want := []string{"exit a1", "exit a", "effect", "enter a", "enter a2"}
	if instance.State() != a2 || !slices.Equal(data.trace, want) {
		t.Fatalf("State/trace = %v/%v, want %v/%v", instance.State(), data.trace, a2, want)
	}
}

func TestActionErrorReportsPhaseAndCommitPoint(t *testing.T) {
	sentinel := errors.New("action failed")
	for _, test := range []struct {
		name      string
		exit      action
		effect    action
		entry     action
		phase     statechart.Phase
		committed bool
		wantState testState
	}{
		{"exit", func(context.Context, statechart.Info[testState, testEvent], *testData) error { return sentinel }, nil, nil, statechart.PhaseExit, false, a},
		{"effect", nil, func(context.Context, statechart.Info[testState, testEvent], *testData) error { return sentinel }, nil, statechart.PhaseEffect, false, a},
		{"entry", nil, nil, func(context.Context, statechart.Info[testState, testEvent], *testData) error { return sentinel }, statechart.PhaseEntry, true, b},
	} {
		t.Run(test.name, func(t *testing.T) {
			chart := statechart.MustCompile(definition{
				States:      []state{{Name: a, Exit: []action{test.exit}}, {Name: b, Entry: []action{test.entry}}},
				Transitions: []transition{{From: a, Event: goB, To: b, Do: test.effect}},
			})
			instance, _ := chart.New(a)
			err := instance.Fire(context.Background(), goB, &testData{})
			var actionErr *statechart.ActionError
			if !errors.As(err, &actionErr) || !errors.Is(err, sentinel) {
				t.Fatalf("err = %#v", err)
			}
			if actionErr.Phase != test.phase || actionErr.Committed != test.committed {
				t.Fatalf("ActionError = %+v", actionErr)
			}
			if text := err.Error(); !strings.Contains(text, test.phase.String()+" action failed") {
				t.Errorf("ActionError text = %q", text)
			}
			if instance.State() != test.wantState {
				t.Fatalf("State = %v, want %v", instance.State(), test.wantState)
			}
		})
	}
}

func TestStringersAndNilActionError(t *testing.T) {
	if got := statechart.Kind(255).String(); got != "Kind(255)" {
		t.Errorf("invalid Kind string = %q", got)
	}
	if got := statechart.Phase(255).String(); got != "Phase(255)" {
		t.Errorf("invalid Phase string = %q", got)
	}
	var actionErr *statechart.ActionError
	if actionErr.Error() != "<nil>" || actionErr.Unwrap() != nil {
		t.Errorf("nil ActionError = %q, unwrap %v", actionErr.Error(), actionErr.Unwrap())
	}
}

func TestPanicPreservesCommitPointAndClearsInFlight(t *testing.T) {
	for _, phase := range []statechart.Phase{statechart.PhaseExit, statechart.PhaseEffect, statechart.PhaseEntry} {
		t.Run(phase.String(), func(t *testing.T) {
			panics := true
			boom := func(context.Context, statechart.Info[testState, testEvent], *testData) error {
				if panics {
					panics = false
					panic("boom")
				}
				return nil
			}
			var exit, effect, entry action
			switch phase {
			case statechart.PhaseExit:
				exit = boom
			case statechart.PhaseEffect:
				effect = boom
			case statechart.PhaseEntry:
				entry = boom
			}
			chart := statechart.MustCompile(definition{
				States:      []state{{Name: a, Exit: []action{exit}}, {Name: b, Entry: []action{entry}}},
				Transitions: []transition{{From: a, Event: goB, To: b, Do: effect}},
			})
			instance, _ := chart.New(a)
			func() {
				defer func() {
					if recover() != "boom" {
						t.Error("Fire did not propagate panic")
					}
				}()
				_ = instance.Fire(context.Background(), goB, &testData{})
			}()
			want := a
			if phase == statechart.PhaseEntry {
				want = b
			}
			if instance.State() != want {
				t.Fatalf("State after panic = %v, want %v", instance.State(), want)
			}
			if phase != statechart.PhaseEntry {
				if err := instance.Fire(context.Background(), goB, &testData{}); err != nil {
					t.Fatalf("Fire after panic: %v", err)
				}
			} else if err := instance.Fire(context.Background(), unknownEvent, &testData{}); errors.Is(err, statechart.ErrInFlight) {
				t.Fatalf("in-flight marker survived panic: %v", err)
			}
		})
	}
}

func TestGoexitClearsInFlightAndPreservesCommitPoint(t *testing.T) {
	chart := statechart.MustCompile(definition{
		States: []state{{Name: a}, {Name: b}},
		Transitions: []transition{{
			From: a, Event: goB, To: b,
			Do: func(context.Context, statechart.Info[testState, testEvent], *testData) error {
				runtime.Goexit()
				return nil
			},
		}},
	})
	instance, _ := chart.New(a)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = instance.Fire(context.Background(), goB, &testData{})
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Goexit callback did not finish")
	}
	if got := instance.State(); got != a {
		t.Fatalf("State after Goexit = %v, want %v", got, a)
	}
	if err := instance.Fire(context.Background(), unknownEvent, &testData{}); !errors.Is(err, statechart.ErrNotPermitted) {
		t.Fatalf("Fire after Goexit = %v, want ordinary refusal", err)
	}
}

func TestConcurrentAndReentrantFireReturnErrInFlight(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	chart := statechart.MustCompile(definition{
		States: []state{{Name: a}, {Name: b}},
		Transitions: []transition{{From: a, Event: goB, To: b, Do: func(context.Context, statechart.Info[testState, testEvent], *testData) error {
			close(started)
			<-release
			return nil
		}}},
	})
	instance, _ := chart.New(a)
	done := make(chan error, 1)
	go func() { done <- instance.Fire(context.Background(), goB, &testData{}) }()
	<-started
	if instance.State() != a {
		t.Fatalf("State during effect = %v, want %v", instance.State(), a)
	}
	if err := instance.Fire(context.Background(), goB, &testData{}); !errors.Is(err, statechart.ErrInFlight) {
		t.Fatalf("overlapping Fire error = %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	var reentrant *statechart.Instance[testState, testEvent, *testData]
	var nested error
	reentrantChart := statechart.MustCompile(definition{
		States: []state{{Name: a}, {Name: b}},
		Transitions: []transition{{From: a, Event: goB, To: b, Do: func(ctx context.Context, _ statechart.Info[testState, testEvent], data *testData) error {
			nested = reentrant.Fire(ctx, goB, data)
			return nil
		}}},
	})
	reentrant, _ = reentrantChart.New(a)
	if err := reentrant.Fire(context.Background(), goB, &testData{}); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(nested, statechart.ErrInFlight) {
		t.Fatalf("nested Fire error = %v", nested)
	}
}

func TestPermittedIsEagerInheritedAndReportsFinalDestinations(t *testing.T) {
	guardCalls := 0
	chart := statechart.MustCompile(definition{
		States: []state{{Name: root}, {Name: a}, {Name: a1}, {Name: b}, {Name: b1}},
		Substates: []statechart.Substate[testState]{
			{Child: a, Parent: root}, {Child: a1, Parent: a},
			{Child: b, Parent: root}, {Child: b1, Parent: b},
		},
		Initials: []statechart.Initial[testState]{{Parent: b, Child: b1}},
		Transitions: []transition{
			{From: a1, Event: goB, To: b, Guard: func(_ context.Context, _ statechart.Info[testState, testEvent], data *testData) error {
				guardCalls++
				if data.allow {
					return nil
				}
				return errors.New("closed")
			}},
			{From: root, Event: goB, To: b},
			{From: a, Event: touch, To: a, Kind: statechart.Internal},
		},
	})
	instance, _ := chart.New(a1)
	data := &testData{allow: true}
	sequence := instance.Permitted(context.Background(), data)
	if guardCalls != 1 {
		t.Fatalf("Permitted called guard %d times before range, want 1", guardCalls)
	}
	data.allow = false
	var events []testEvent
	destinations := make(map[testEvent]testState)
	for event, destination := range sequence {
		events = append(events, event)
		destinations[event] = destination
	}
	if want := []testEvent{goB, touch}; !slices.Equal(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	if destinations[goB] != b1 || destinations[touch] != a1 {
		t.Fatalf("destinations = %v", destinations)
	}
	if guardCalls != 1 {
		t.Fatalf("ranging eager snapshot reran guard; calls = %d", guardCalls)
	}

	// With the leaf row now declined, the inherited row for the same event is
	// still selected and reports the same initial-resolved destination.
	fallbacks := make(map[testEvent]testState)
	for event, destination := range instance.Permitted(context.Background(), data) {
		fallbacks[event] = destination
	}
	if fallbacks[goB] != b1 || fallbacks[touch] != a1 || guardCalls != 2 {
		t.Fatalf("inherited fallback = %v; guard calls = %d", fallbacks, guardCalls)
	}
}

func TestInstanceIsRaceSafeForStateAndRefusesUnknownEvent(t *testing.T) {
	chart := statechart.MustCompile(definition{
		States:      []state{{Name: a}, {Name: b}},
		Transitions: []transition{{From: a, Event: goB, To: b}},
	})
	instance, _ := chart.New(a)
	var wait sync.WaitGroup
	for range 20 {
		wait.Go(func() { _ = instance.State() })
	}
	wait.Wait()
	err := instance.Fire(context.Background(), unknownEvent, &testData{})
	if !errors.Is(err, statechart.ErrNotPermitted) || instance.State() != a {
		t.Fatalf("Fire = %v, state = %v", err, instance.State())
	}
}
