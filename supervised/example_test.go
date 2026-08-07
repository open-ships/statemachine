package supervised_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/open-ships/statemachine/supervised"
)

func ExampleSupervisor() {
	type State string
	type Event string
	type IO struct {
		InterlockClosed bool
		AtTarget        bool
	}

	const (
		Idle State = "idle"
		Work State = "work"
		Move Event = "move"
	)

	requireInterlock := func(_ context.Context, _ supervised.Attempt[State, Event], io IO) error {
		if !io.InterlockClosed {
			return errors.New("interlock open")
		}
		return nil
	}
	invariant := func(_ context.Context, _ supervised.Change[State, Event], io IO) error {
		if !io.InterlockClosed {
			return errors.New("interlock opened")
		}
		return nil
	}
	acceptChange := func(context.Context, supervised.Change[State, Event], IO) error { return nil }
	reconcile := func(_ context.Context, snapshot supervised.Snapshot[State], io IO) error {
		if snapshot.State == Work && !io.AtTarget {
			return errors.New("logical and physical position disagree")
		}
		return nil
	}

	machine := supervised.MustCompile(supervised.Definition[State, Event, IO]{
		ID:      "arm-position/v1",
		Initial: Idle,
		States: []supervised.State[State, Event]{
			{Name: Idle},
			{Name: Work, Terminal: true},
		},
		Events: []Event{Move},
		Transitions: []supervised.Transition[State, Event, IO]{
			{
				ID: "idle-to-work", From: Idle, Event: Move, To: Work,
				Issue: func(context.Context, supervised.Change[State, Event], IO) error {
					return nil // send a request; do not claim physical completion
				},
				Verify: func(_ context.Context, _ supervised.Change[State, Event], io IO) error {
					if !io.AtTarget {
						return errors.New("target not reached")
					}
					return nil
				},
			},
		},
		Preconditions:  []supervised.Precondition[State, Event, IO]{requireInterlock},
		Invariants:     []supervised.Check[State, Event, IO]{invariant},
		Postconditions: []supervised.Check[State, Event, IO]{acceptChange},
		Reconcile:      []supervised.Reconciler[State, IO]{reconcile},
	})

	run, _ := supervised.New(machine, supervised.Limits{
		OperationTimeout:    time.Second,
		VerificationTimeout: 5 * time.Second,
	})
	_ = run.Start(context.Background(), IO{InterlockClosed: true})
	fmt.Println("start:", run.Status().Mode)

	issued := run.Issue(context.Background(), Move, IO{InterlockClosed: true})
	fmt.Println("issue:", issued.TransitionID, issued.Committed)

	committed := run.Verify(context.Background(), issued.Attempt, IO{
		InterlockClosed: true,
		AtTarget:        true,
	})
	fmt.Println("verify:", run.Snapshot().State, committed.Committed, committed.Revision)

	// Output:
	// start: ready
	// issue: idle-to-work false
	// verify: work true 1
}
