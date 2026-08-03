package statechart_test

import (
	"context"
	"fmt"

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

	run, _ := chart.New(Call) // restoration follows initials but runs no entry action
	fmt.Println("initial:", run.State())

	command := &Command{}
	_ = run.Fire(context.Background(), Answer, command)
	fmt.Println("answer:", run.State(), command.Trace)

	command.Trace = nil
	_ = run.Fire(context.Background(), Restart, command) // inherited from Call
	fmt.Println("restart:", run.State(), command.Trace)

	command.Trace = nil
	_ = run.Fire(context.Background(), Hangup, command) // inherited from Call
	fmt.Println("hangup:", run.State(), command.Trace)

	// Output:
	// initial: ringing
	// answer: active [exit ringing answer enter active]
	// restart: ringing [exit active exit call restart enter call enter ringing]
	// hangup: idle [exit ringing exit call hangup enter idle]
}
