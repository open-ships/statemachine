package queued_test

import (
	"context"
	"fmt"

	"github.com/open-ships/statemachine"
	"github.com/open-ships/statemachine/queued"
)

func Example() {
	type State string
	type Event string
	type Command struct{ Trace []string }

	const (
		Pending    State = "pending"
		Authorized State = "authorized"
		Settled    State = "settled"

		Authorize Event = "authorize"
		Capture   Event = "capture"
	)

	machine := statemachine.MustCompile([]statemachine.Transition[State, Event, *Command]{
		{
			From: Pending, Event: Authorize, To: Authorized,
			Do: func(ctx context.Context, command *Command) error {
				command.Trace = append(command.Trace, "authorized")
				return queued.Enqueue(ctx, Capture, command)
			},
		},
		{
			From: Authorized, Event: Capture, To: Settled,
			Do: func(_ context.Context, command *Command) error {
				command.Trace = append(command.Trace, "captured")
				return nil
			},
		},
	})

	run := queued.New(machine, Pending)
	command := &Command{}
	state, err := run.Fire(context.Background(), Authorize, command)
	fmt.Println(state, err)
	fmt.Println(command.Trace)

	// Output:
	// settled <nil>
	// [authorized captured]
}
