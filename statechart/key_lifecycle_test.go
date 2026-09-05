package statechart_test

import (
	"context"
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/open-ships/statemachine/statechart"
)

func TestZeroChartAndInstanceRejectInvalidKeys(t *testing.T) {
	var chart statechart.Chart[any, any, struct{}]
	var instance statechart.Instance[any, any, struct{}]
	for _, invalid := range []any{[]int{1}, math.NaN(), struct{ Value any }{[]int{1}}} {
		if _, err := chart.New(invalid); !errors.Is(err, statechart.ErrInvalidKey) {
			t.Fatalf("New(%v) = %v", invalid, err)
		}
		if state, err := instance.Fire(context.Background(), invalid, struct{}{}); state != nil || !errors.Is(err, statechart.ErrInvalidKey) {
			t.Fatalf("Fire(%v) = %v, %v", invalid, state, err)
		}
		if _, ok := chart.Position(invalid); ok {
			t.Fatal("zero Chart produced a Position")
		}
		if _, ok := chart.Destination(invalid); ok {
			t.Fatal("zero Chart produced a Destination")
		}
		for range chart.Arrows(invalid) {
			t.Fatal("zero Chart produced an Arrow")
		}
	}
	if _, err := instance.Fire(context.Background(), "valid", struct{}{}); !errors.Is(err, statechart.ErrNotPermitted) {
		t.Fatalf("valid event on zero Instance = %v", err)
	}
}

func TestStatechartRejectsNaNAtEveryDeclarationAndQuery(t *testing.T) {
	nan := math.NaN()
	type def = statechart.Definition[float64, float64, struct{}]
	type node = statechart.State[float64, float64, struct{}]
	type row = statechart.Transition[float64, float64, struct{}]
	cases := []def{
		{States: []node{{Name: nan}, {Name: nan}}},
		{States: []node{{Name: 0}, {Name: 1}}, Substates: []statechart.Substate[float64]{{Child: nan, Parent: 0}}},
		{States: []node{{Name: 0}, {Name: 1}}, Initials: []statechart.Initial[float64]{{Parent: 0, Child: nan}}},
		{States: []node{{Name: 0}, {Name: 1}}, Transitions: []row{{From: nan, Event: 0, To: 1}}},
		{States: []node{{Name: 0}, {Name: 1}}, Transitions: []row{{From: 0, Event: nan, To: 1}}},
		{States: []node{{Name: 0}, {Name: 1}}, Transitions: []row{{From: 0, Event: 0, To: nan}}},
	}
	for index, definition := range cases {
		if _, err := statechart.Compile(definition); !errors.Is(err, statechart.ErrInvalidKey) {
			t.Errorf("definition %d: %v", index, err)
		}
	}
	type nested struct{ Value [1]complex128 }
	if _, err := statechart.Compile(statechart.Definition[nested, int, struct{}]{States: []statechart.State[nested, int, struct{}]{{Name: nested{[1]complex128{complex(0, nan)}}}}}); !errors.Is(err, statechart.ErrInvalidKey) {
		t.Fatalf("nested key: %v", err)
	}
	chart := statechart.MustCompile(def{States: []node{{Name: 0}, {Name: 1}}, Transitions: []row{{From: 0, Event: 0, To: 1}}})
	if _, err := chart.New(nan); !errors.Is(err, statechart.ErrInvalidKey) {
		t.Fatal(err)
	}
	if _, ok := chart.Position(nan); ok {
		t.Fatal("Position accepted NaN")
	}
	if _, ok := chart.Destination(nan); ok {
		t.Fatal("Destination accepted NaN")
	}
	for range chart.Arrows(nan) {
		t.Fatal("Arrows accepted NaN")
	}
	position, _ := chart.Position(0)
	if _, ok := position.Status(nan); ok {
		t.Fatal("Status accepted NaN")
	}
	instance, _ := chart.New(0)
	if got, err := instance.Fire(context.Background(), nan, struct{}{}); got != 0 || !errors.Is(err, statechart.ErrInvalidKey) {
		t.Fatalf("Fire = %v, %v", got, err)
	}
	if got, err := instance.Fire(context.Background(), 0, struct{}{}); got != 1 || err != nil {
		t.Fatalf("valid Fire = %v, %v", got, err)
	}
}

func TestActionErrorCountsOnlyCompletedCallbacksAcrossNodes(t *testing.T) {
	sentinel := errors.New("lifecycle failed")
	for _, phase := range []statechart.Phase{statechart.PhaseExit, statechart.PhaseEntry} {
		t.Run(phase.String(), func(t *testing.T) {
			succeeded := 0
			success := func(context.Context, statechart.Info[testState, testEvent], *testData) error { succeeded++; return nil }
			fail := func(context.Context, statechart.Info[testState, testEvent], *testData) error { return sentinel }
			nodes := []state{{Name: a}, {Name: a1}, {Name: b}, {Name: b1}}
			failingState := a
			if phase == statechart.PhaseExit {
				nodes[1].Exit = []action{nil, success, nil, success}
				nodes[0].Exit = []action{nil, success, nil, fail}
			} else {
				nodes[2].Entry = []action{nil, success, nil, success}
				nodes[3].Entry = []action{nil, success, nil, fail}
				failingState = b1
			}
			chart := statechart.MustCompile(definition{States: nodes, Substates: []statechart.Substate[testState]{{Child: a1, Parent: a}, {Child: b1, Parent: b}}, Initials: []statechart.Initial[testState]{{Parent: b, Child: b1}}, Transitions: []transition{{From: a, Event: goB, To: b}}})
			observed := 0
			instance, _ := chart.NewWithObservers(a1, func(context.Context, statechart.Observation[testState, testEvent], *testData) { observed++ })
			_, err := instance.Fire(context.Background(), goB, &testData{})
			var detail *statechart.ActionError
			if !errors.As(err, &detail) || !errors.Is(err, sentinel) {
				t.Fatal(err)
			}
			if detail.Phase != phase || detail.State != failingState || detail.ActionIndex != 3 || detail.Completed != 3 || succeeded != 3 || detail.Committed {
				t.Fatalf("failure = %+v, successful calls = %d", detail, succeeded)
			}
			if instance.State() != a1 || observed != 0 {
				t.Fatalf("failed lifecycle committed state/observations: %v/%d", instance.State(), observed)
			}
		})
	}
}

func TestCompiledHierarchyHandlesReorderedDeclarationsAndDeepInitialChains(t *testing.T) {
	const depth = 2048
	definition := statechart.Definition[int, int, struct{}]{}
	for state := depth; state >= 0; state-- {
		definition.States = append(definition.States, statechart.State[int, int, struct{}]{Name: state})
		if state > 0 {
			definition.Substates = append(definition.Substates, statechart.Substate[int]{Child: state, Parent: state - 1})
			definition.Initials = append(definition.Initials, statechart.Initial[int]{Parent: state - 1, Child: state})
		}
	}
	definition.States = append(definition.States, statechart.State[int, int, struct{}]{Name: depth + 1})
	definition.Transitions = []statechart.Transition[int, int, struct{}]{{From: 0, Event: 0, To: depth + 1}, {From: depth + 1, Event: 1, To: 0}}
	chart, err := statechart.Compile(definition)
	if err != nil {
		t.Fatal(err)
	}
	instance, err := chart.New(0)
	if err != nil || instance.State() != depth {
		t.Fatalf("New = %v, %v", instance, err)
	}
	position, _ := instance.Position()
	path := slices.Collect(position.Path())
	if len(path) != depth+1 || path[0] != depth || path[depth] != 0 {
		t.Fatalf("deep path: length %d", len(path))
	}
	if status, ok := position.Status(0); !ok || status != statechart.StatusEnclosing {
		t.Fatalf("root status: %v/%v", status, ok)
	}
	if got, err := instance.Fire(context.Background(), 0, struct{}{}); got != depth+1 || err != nil {
		t.Fatalf("exit forest: %v, %v", got, err)
	}
	if got, err := instance.Fire(context.Background(), 1, struct{}{}); got != depth || err != nil {
		t.Fatalf("enter forest: %v, %v", got, err)
	}
	if active, _ := position.Active(); active != depth {
		t.Fatal("saved Position mutated")
	}
}
