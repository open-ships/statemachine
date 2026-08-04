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

	"github.com/open-ships/statemachine"
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

func TestPositionProjectsTheCompiledHierarchy(t *testing.T) {
	chart := statechart.MustCompile(definition{
		States:    baseStates(),
		Substates: hierarchy(),
		Initials: []statechart.Initial[testState]{
			{Parent: root, Child: a},
			{Parent: a, Child: a1},
			{Parent: b, Child: b1},
		},
	})

	states := slices.Collect(chart.States())
	wantStates := []testState{root, a, a1, a2, b, b1, other}
	if !slices.Equal(states, wantStates) {
		t.Fatalf("States = %v, want %v", states, wantStates)
	}
	var nilChart *statechart.Chart[testState, testEvent, *testData]
	if nilStates := slices.Collect(nilChart.States()); nilStates != nil {
		t.Fatalf("nil Chart States = %v, want nil", nilStates)
	}

	position, ok := chart.Position(a1)
	if !ok {
		t.Fatal("Position rejected a declared active state")
	}
	if active, valid := position.Active(); !valid || active != a1 {
		t.Fatalf("Active = (%v, %v), want (%v, true)", active, valid, a1)
	}
	if path := slices.Collect(position.Path()); !slices.Equal(path, []testState{a1, a, root}) {
		t.Fatalf("Path = %v", path)
	}
	for state, want := range map[testState]statechart.Status{
		a1:    statechart.StatusActive,
		a:     statechart.StatusEnclosing,
		root:  statechart.StatusEnclosing,
		a2:    statechart.StatusInactive,
		b:     statechart.StatusInactive,
		other: statechart.StatusInactive,
	} {
		if got, known := position.Status(state); !known || got != want {
			t.Errorf("Status(%v) = (%v, %v), want (%v, true)", state, got, known, want)
		}
	}
	if _, known := position.Status(testState("missing")); known {
		t.Fatal("Status accepted an undeclared state")
	}
	if _, ok := chart.Position(testState("missing")); ok {
		t.Fatal("Position accepted an undeclared active state")
	}
	if destination, ok := chart.Destination(root); !ok || destination != a1 {
		t.Fatalf("Destination(root) = (%v, %v), want (%v, true)", destination, ok, a1)
	}
	if destination, ok := chart.Destination(other); !ok || destination != other {
		t.Fatalf("Destination(other) = (%v, %v), want (%v, true)", destination, ok, other)
	}
	if _, ok := chart.Destination(testState("missing")); ok {
		t.Fatal("Destination accepted an undeclared state")
	}

	var zero statechart.Position[testState]
	if _, ok := zero.Active(); ok || slices.Collect(zero.Path()) != nil {
		t.Fatalf("zero Position is valid")
	}
	if _, known := zero.Status(root); known {
		t.Fatal("zero Position knows a state")
	}
}

func TestArrowsExposeGuardFreeInheritedCandidates(t *testing.T) {
	guardCalls := 0
	guard := func(context.Context, statechart.Info[testState, testEvent], *testData) error {
		guardCalls++
		return errors.New("not evaluated")
	}
	chart := statechart.MustCompile(definition{
		States:    baseStates(),
		Substates: hierarchy(),
		Initials: []statechart.Initial[testState]{
			{Parent: a, Child: a1},
			{Parent: b, Child: b1},
		},
		Transitions: []transition{
			{From: a1, Event: goB, To: b, Guard: guard},
			{From: a1, Event: goB, To: other, Guard: guard},
			{From: a, Event: goB, To: b},
			{From: root, Event: goB, To: other},
			{From: a, Event: touch, To: a, Kind: statechart.Internal},
			{From: a, Event: reset, To: a, Kind: statechart.Reentry},
		},
	})

	got := slices.Collect(chart.Arrows(a1))
	want := []statechart.Arrow[testState, testEvent]{
		{Source: a1, Handler: a1, Target: b, Destination: b1, Event: goB, Kind: statechart.External, Guarded: true},
		{Source: a1, Handler: a1, Target: other, Destination: other, Event: goB, Kind: statechart.External, Guarded: true},
		{Source: a1, Handler: a, Target: b, Destination: b1, Event: goB, Kind: statechart.External},
		{Source: a1, Handler: a, Target: a, Destination: a1, Event: touch, Kind: statechart.Internal},
		{Source: a1, Handler: a, Target: a, Destination: a1, Event: reset, Kind: statechart.Reentry},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("Arrows = %+v, want %+v", got, want)
	}
	if guardCalls != 0 {
		t.Fatalf("Arrows called Guards %d times", guardCalls)
	}
	if unknown := slices.Collect(chart.Arrows(testState("missing"))); unknown != nil {
		t.Fatalf("unknown Arrows = %+v, want nil", unknown)
	}
	var nilChart *statechart.Chart[testState, testEvent, *testData]
	if nilArrows := slices.Collect(nilChart.Arrows(a1)); nilArrows != nil {
		t.Fatalf("nil Chart Arrows = %+v, want nil", nilArrows)
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
	var observations []statechart.Observation[testState, testEvent]
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
	instance, _ := chart.NewWithObservers(a1, func(
		_ context.Context, observation statechart.Observation[testState, testEvent], _ *testData,
	) {
		observations = append(observations, observation)
	})
	data := &testData{}
	if err := instance.Fire(context.Background(), reset, data); err != nil {
		t.Fatal(err)
	}
	want := []string{"exit a1", "exit a", "effect", "enter a", "enter a2"}
	if instance.State() != a2 || !slices.Equal(data.trace, want) {
		t.Fatalf("State/trace = %v/%v, want %v/%v", instance.State(), data.trace, a2, want)
	}
	wantMoves := []struct {
		move  statemachine.Move
		state testState
	}{
		{statemachine.Exited, a1},
		{statemachine.Exited, a},
		{statemachine.Entered, a},
		{statemachine.Entered, a2},
	}
	if len(observations) != len(wantMoves) {
		t.Fatalf("observations = %+v", observations)
	}
	for index, want := range wantMoves {
		got := observations[index]
		if got.Move != want.move || got.State != want.state || got.Remaining != uint64(len(wantMoves)-index-1) {
			t.Errorf("observation[%d] = %+v, want %v %v", index, got, want.move, want.state)
		}
	}
}

func TestStatechartObservesEveryCommittedNodeAfterEntryProcessing(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "request")
	data := &testData{}
	var instance *statechart.Instance[testState, testEvent, *testData]
	var observations []statechart.Observation[testState, testEvent]
	var traces [][]string
	var activeStates []testState
	var nestedErrors []error
	var contexts []any
	var observedData []*testData

	observer := func(
		observerCtx context.Context,
		observation statechart.Observation[testState, testEvent],
		observed *testData,
	) {
		observations = append(observations, observation)
		traces = append(traces, slices.Clone(observed.trace))
		position, ok := instance.Position()
		contexts = append(contexts, observerCtx.Value(contextKey{}))
		observedData = append(observedData, observed)
		if !ok {
			return
		}
		active, _ := position.Active()
		activeStates = append(activeStates, active)
		nestedErrors = append(nestedErrors, instance.Fire(observerCtx, goB, observed))
	}

	chart := statechart.MustCompile(definition{
		States: []state{
			{Name: root},
			{Name: a, Exit: []action{record("exit a")}},
			{Name: a1, Exit: []action{record("exit a1")}},
			{Name: b, Entry: []action{record("enter b")}},
			{Name: b1, Entry: []action{record("enter b1")}},
		},
		Substates: []statechart.Substate[testState]{
			{Child: a, Parent: root}, {Child: a1, Parent: a},
			{Child: b, Parent: root}, {Child: b1, Parent: b},
		},
		Initials:    []statechart.Initial[testState]{{Parent: b, Child: b1}},
		Transitions: []transition{{From: a, Event: goB, To: b, Do: record("effect")}},
	})
	var err error
	instance, err = chart.NewWithObservers(a1, nil, observer)
	if err != nil {
		t.Fatal(err)
	}
	if len(observations) != 0 {
		t.Fatal("restoration emitted observations")
	}
	if err := instance.Fire(ctx, goB, data); err != nil {
		t.Fatal(err)
	}

	info := statechart.Info[testState, testEvent]{
		Source: a1, Handler: a, Target: b, Destination: b1, Event: goB, Kind: statechart.External,
	}
	want := []statechart.Observation[testState, testEvent]{
		{Observation: statemachine.Observation[testState, testEvent]{Seq: 1, Step: 1, Run: 1, Remaining: 3, Move: statemachine.Exited, State: a1, Event: goB}, Info: info},
		{Observation: statemachine.Observation[testState, testEvent]{Seq: 2, Step: 1, Run: 1, Remaining: 2, Move: statemachine.Exited, State: a, Event: goB}, Info: info},
		{Observation: statemachine.Observation[testState, testEvent]{Seq: 3, Step: 1, Run: 1, Remaining: 1, Move: statemachine.Entered, State: b, Event: goB}, Info: info},
		{Observation: statemachine.Observation[testState, testEvent]{Seq: 4, Step: 1, Run: 1, Move: statemachine.Entered, State: b1, Event: goB}, Info: info},
	}
	if !slices.Equal(observations, want) {
		t.Fatalf("observations = %+v, want %+v", observations, want)
	}
	wantTrace := []string{"exit a1", "exit a", "effect", "enter b", "enter b1"}
	for index := range observations {
		if !slices.Equal(traces[index], wantTrace) || activeStates[index] != b1 || nestedErrors[index] != statechart.ErrInFlight ||
			contexts[index] != "request" || observedData[index] != data {
			t.Errorf("callback[%d] trace/state/nested/context/data = %v/%v/%v/%v/%p", index, traces[index], activeStates[index], nestedErrors[index], contexts[index], observedData[index])
		}
	}
}

func TestStatechartCommittedEntryFailuresStillObserve(t *testing.T) {
	sentinel := errors.New("entry failed")
	for _, test := range []struct {
		name  string
		entry action
	}{
		{"error", func(context.Context, statechart.Info[testState, testEvent], *testData) error { return sentinel }},
		{"panic", func(context.Context, statechart.Info[testState, testEvent], *testData) error { panic(sentinel) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			var observations []statechart.Observation[testState, testEvent]
			chart := statechart.MustCompile(definition{
				States:      []state{{Name: a}, {Name: b, Entry: []action{test.entry}}},
				Transitions: []transition{{From: a, Event: goB, To: b}},
			})
			instance, _ := chart.NewWithObservers(a, func(
				_ context.Context, observation statechart.Observation[testState, testEvent], _ *testData,
			) {
				observations = append(observations, observation)
			})

			var err error
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				err = instance.Fire(context.Background(), goB, &testData{})
			}()
			if test.name == "error" {
				var actionErr *statechart.ActionError
				if !errors.As(err, &actionErr) || !actionErr.Committed || !errors.Is(err, sentinel) {
					t.Fatalf("entry error = %#v", err)
				}
			} else if recovered != sentinel {
				t.Fatalf("recovered = %#v, want sentinel", recovered)
			}
			if instance.State() != b || len(observations) != 2 || observations[1].Remaining != 0 {
				t.Fatalf("state/observations = %v/%+v", instance.State(), observations)
			}
		})
	}
}

func TestStatechartCommittedEntryGoexitStillObservesAndObserverFailuresAreIsolated(t *testing.T) {
	var delivered []statechart.Observation[testState, testEvent]
	panicCalls := 0
	goexitCalls := 0
	chart := statechart.MustCompile(definition{
		States: []state{{Name: a}, {Name: b, Entry: []action{func(context.Context, statechart.Info[testState, testEvent], *testData) error {
			runtime.Goexit()
			return nil
		}}}},
		Transitions: []transition{{From: a, Event: goB, To: b}},
	})
	instance, _ := chart.NewWithObservers(
		a,
		func(context.Context, statechart.Observation[testState, testEvent], *testData) {
			panicCalls++
			panic("observer")
		},
		func(context.Context, statechart.Observation[testState, testEvent], *testData) {
			goexitCalls++
			runtime.Goexit()
		},
		func(_ context.Context, observation statechart.Observation[testState, testEvent], _ *testData) {
			delivered = append(delivered, observation)
		},
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = instance.Fire(context.Background(), goB, &testData{})
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("entry Goexit stranded Fire")
	}
	if instance.State() != b || len(delivered) != 2 || delivered[1].Remaining != 0 {
		t.Fatalf("state/delivered = %v/%+v", instance.State(), delivered)
	}
	if panicCalls != 2 || goexitCalls != 2 {
		t.Fatalf("failed observer calls = panic %d, Goexit %d; want 2 each", panicCalls, goexitCalls)
	}
	if err := instance.Fire(context.Background(), unknownEvent, &testData{}); errors.Is(err, statechart.ErrInFlight) {
		t.Fatalf("in-flight survived Goexit: %v", err)
	}
}

func TestStatechartPrecommitFailuresAndInternalTransitionsAreObservationSilent(t *testing.T) {
	sentinel := errors.New("failed")
	for _, test := range []struct {
		name   string
		exit   action
		effect action
	}{
		{"exit", func(context.Context, statechart.Info[testState, testEvent], *testData) error { return sentinel }, nil},
		{"effect", nil, func(context.Context, statechart.Info[testState, testEvent], *testData) error { return sentinel }},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			chart := statechart.MustCompile(definition{
				States:      []state{{Name: a, Exit: []action{test.exit}}, {Name: b}},
				Transitions: []transition{{From: a, Event: goB, To: b, Do: test.effect}},
			})
			instance, _ := chart.NewWithObservers(a, func(context.Context, statechart.Observation[testState, testEvent], *testData) { calls++ })
			if err := instance.Fire(context.Background(), goB, &testData{}); !errors.Is(err, sentinel) {
				t.Fatalf("Fire = %v", err)
			}
			if calls != 0 || instance.State() != a {
				t.Fatalf("calls/state = %d/%v", calls, instance.State())
			}
		})
	}

	calls := 0
	chart := statechart.MustCompile(definition{
		States:      []state{{Name: a}},
		Transitions: []transition{{From: a, Event: touch, To: a, Kind: statechart.Internal}},
	})
	instance, _ := chart.NewWithObservers(a, func(context.Context, statechart.Observation[testState, testEvent], *testData) { calls++ })
	if err := instance.Fire(context.Background(), touch, &testData{}); err != nil || calls != 0 {
		t.Fatalf("internal Fire/calls = %v/%d", err, calls)
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

func benchmarkChart() *statechart.Chart[testState, testEvent, *testData] {
	return statechart.MustCompile(definition{
		States:    baseStates(),
		Substates: hierarchy(),
		Initials: []statechart.Initial[testState]{
			{Parent: a, Child: a1},
			{Parent: b, Child: b1},
		},
		Transitions: []transition{
			{From: a, Event: goB, To: b},
			{From: b, Event: reset, To: a},
		},
	})
}

func BenchmarkPositionPath(b *testing.B) {
	chart := benchmarkChart()
	position, _ := chart.Position(a1)
	b.ReportAllocs()
	for b.Loop() {
		for range position.Path() {
		}
	}
}

func BenchmarkInstancePositionPath(b *testing.B) {
	chart := benchmarkChart()
	instance, _ := chart.New(a1)
	b.ReportAllocs()
	for b.Loop() {
		position, _ := instance.Position()
		for range position.Path() {
		}
	}
}

func BenchmarkPositionStatus(b *testing.B) {
	chart := benchmarkChart()
	position, _ := chart.Position(a1)
	b.ReportAllocs()
	for b.Loop() {
		_, _ = position.Status(root)
		_, _ = position.Status(other)
	}
}

func BenchmarkArrows(b *testing.B) {
	chart := benchmarkChart()
	b.ReportAllocs()
	for b.Loop() {
		for range chart.Arrows(a1) {
		}
	}
}

func BenchmarkStatechartFire(b *testing.B) {
	chart := benchmarkChart()
	instance, _ := chart.New(a1)
	data := &testData{}
	b.ReportAllocs()
	for b.Loop() {
		_ = instance.Fire(context.Background(), goB, data)
		_ = instance.Fire(context.Background(), reset, data)
	}
}

func BenchmarkStatechartFireObserved(b *testing.B) {
	chart := benchmarkChart()
	instance, _ := chart.NewWithObservers(
		a1,
		func(context.Context, statechart.Observation[testState, testEvent], *testData) {},
	)
	data := &testData{}
	b.ReportAllocs()
	for b.Loop() {
		_ = instance.Fire(context.Background(), goB, data)
		_ = instance.Fire(context.Background(), reset, data)
	}
}
