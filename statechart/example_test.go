package statechart_test

import (
	"context"
	"fmt"

	"github.com/open-ships/statemachine"
	"github.com/open-ships/statemachine/statechart"
)

func Example() {
	type State string
	type Event string
	type Command struct{ Trace []string }

	const (
		Call    State = "call"
		Ringing State = "ringing"
		Active  State = "active"
		Idle    State = "idle"

		Answer  Event = "answer"
		Restart Event = "restart"
		Hangup  Event = "hangup"
	)

	type Action = statechart.Action[State, Event, *Command]
	record := func(label string) Action {
		return func(_ context.Context, _ statechart.Info[State, Event], command *Command) error {
			command.Trace = append(command.Trace, label)
			return nil
		}
	}

	chart := statechart.MustCompile(statechart.Definition[State, Event, *Command]{
		States: []statechart.State[State, Event, *Command]{
			{Name: Call, Entry: []Action{record("enter call")}, Exit: []Action{record("exit call")}},
			{Name: Ringing, Entry: []Action{record("enter ringing")}, Exit: []Action{record("exit ringing")}},
			{Name: Active, Entry: []Action{record("enter active")}, Exit: []Action{record("exit active")}},
			{Name: Idle, Entry: []Action{record("enter idle")}},
		},
		Substates: []statechart.Substate[State]{
			{Child: Ringing, Parent: Call},
			{Child: Active, Parent: Call},
		},
		Initials: []statechart.Initial[State]{
			{Parent: Call, Child: Ringing},
		},
		Transitions: []statechart.Transition[State, Event, *Command]{
			{From: Ringing, Event: Answer, To: Active, Do: record("answer")},
			{From: Call, Event: Restart, To: Call, Kind: statechart.Reentry, Do: record("restart")},
			{From: Call, Event: Hangup, To: Idle, Do: record("hangup")},
		},
	})

	run, err := chart.New(Call) // restoration follows initials but runs no entry action
	if err != nil {
		panic(err)
	}
	fmt.Println("initial:", run.State())

	command := &Command{}
	if _, err := run.Fire(context.Background(), Answer, command); err != nil {
		panic(err)
	}
	fmt.Println("answer:", run.State(), command.Trace)

	command.Trace = nil
	if _, err := run.Fire(context.Background(), Restart, command); err != nil {
		panic(err)
	} // inherited from Call
	fmt.Println("restart:", run.State(), command.Trace)

	command.Trace = nil
	if _, err := run.Fire(context.Background(), Hangup, command); err != nil {
		panic(err)
	} // inherited from Call
	fmt.Println("hangup:", run.State(), command.Trace)

	// Output:
	// initial: ringing
	// answer: active [exit ringing answer enter active]
	// restart: ringing [exit active exit call restart enter call enter ringing]
	// hangup: idle [exit ringing exit call hangup enter idle]
}

func ExamplePosition() {
	type State string
	type Event string

	const (
		Call    State = "call"
		Ringing State = "ringing"
		Active  State = "active"
		Idle    State = "idle"
	)

	chart := statechart.MustCompile(statechart.Definition[State, Event, struct{}]{
		States: []statechart.State[State, Event, struct{}]{
			{Name: Call}, {Name: Ringing}, {Name: Active}, {Name: Idle},
		},
		Substates: []statechart.Substate[State]{
			{Child: Ringing, Parent: Call},
			{Child: Active, Parent: Call},
		},
	})
	position, _ := chart.Position(Ringing)
	for state := range chart.States() {
		status, _ := position.Status(state)
		fmt.Println(state, status)
	}

	// Output:
	// call enclosing
	// ringing active
	// active inactive
	// idle inactive
}

func ExampleObserver_census() {
	type State string
	type Event string
	type Command struct{ ID string }

	const (
		Call    State = "call"
		Ringing State = "ringing"
		Idle    State = "idle"
		Hangup  Event = "hangup"
	)

	chart := statechart.MustCompile(statechart.Definition[State, Event, *Command]{
		States: []statechart.State[State, Event, *Command]{
			{Name: Call}, {Name: Ringing}, {Name: Idle},
		},
		Substates:   []statechart.Substate[State]{{Child: Ringing, Parent: Call}},
		Transitions: []statechart.Transition[State, Event, *Command]{{From: Call, Event: Hangup, To: Idle}},
	})

	// A delta stream cannot reconstruct aggregates that existed before the
	// observer. Seed their loaded positions first.
	counts := map[State]int{}
	loaded, _ := chart.Position(Ringing)
	for state := range loaded.Path() {
		counts[state]++
	}

	observer := func(_ context.Context, observation statechart.Observation[State, Event], command *Command) {
		delta := -1
		if observation.Move == statemachine.Entered {
			delta = 1
		}
		counts[observation.State] += delta
		fmt.Println(command.ID, observation.Seq, observation.Move, observation.State)
	}

	run, err := chart.NewWithObservers(Ringing, observer)
	if err != nil {
		panic(err)
	}
	if _, err := run.Fire(context.Background(), Hangup, &Command{ID: "A1"}); err != nil {
		panic(err)
	}
	fmt.Println("census:", counts[Call], counts[Ringing], counts[Idle])

	// Output:
	// A1 1 exited ringing
	// A1 2 exited call
	// A1 3 entered idle
	// census: 0 0 1
}

// External computes lifecycle paths from the active Source to Target. An
// inherited handler does not imply that its own lifecycle runs again.
func ExampleKind_inheritedHandler() {
	var trace []string
	record := func(label string) statechart.Action[string, string, struct{}] {
		return func(context.Context, statechart.Info[string, string], struct{}) error {
			trace = append(trace, label)
			return nil
		}
	}
	chart := statechart.MustCompile(statechart.Definition[string, string, struct{}]{
		States: []statechart.State[string, string, struct{}]{
			{Name: "parent", Entry: []statechart.Action[string, string, struct{}]{record("enter parent")}, Exit: []statechart.Action[string, string, struct{}]{record("exit parent")}},
			{Name: "leaf", Entry: []statechart.Action[string, string, struct{}]{record("enter leaf")}, Exit: []statechart.Action[string, string, struct{}]{record("exit leaf")}},
		},
		Substates: []statechart.Substate[string]{{Child: "leaf", Parent: "parent"}},
		Initials:  []statechart.Initial[string]{{Parent: "parent", Child: "leaf"}},
		Transitions: []statechart.Transition[string, string, struct{}]{
			{From: "parent", Event: "external", To: "leaf", Do: record("effect")},
			{From: "parent", Event: "reentry", To: "parent", Kind: statechart.Reentry, Do: record("effect")},
		},
	})
	instance, err := chart.New("leaf")
	if err != nil {
		panic(err)
	}
	if _, err := instance.Fire(context.Background(), "external", struct{}{}); err != nil {
		panic(err)
	}
	fmt.Println("external:", trace)
	trace = nil
	if _, err := instance.Fire(context.Background(), "reentry", struct{}{}); err != nil {
		panic(err)
	}
	fmt.Println("reentry:", trace)
	// Output:
	// external: [effect]
	// reentry: [exit leaf exit parent effect enter parent enter leaf]
}
