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
	reconcile := func(_ context.Context, snapshot supervised.Snapshot[State, Event], io IO) error {
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
		Reconcile:      []supervised.Reconciler[State, Event, IO]{reconcile},
	})

	// A Machine with external Issue actions requires a durable Journal: the
	// in-doubt Snapshot is persisted before Issue runs and the closure
	// Snapshot after every commit, fault, and recovery, so a restart can
	// prove what was in flight. This example journals to memory; production
	// integrations write to durable storage.
	var journal supervised.Snapshot[State, Event]
	run, err := supervised.NewWithOptions(machine, supervised.Options[State, Event]{
		Limits: supervised.Limits{
			OperationTimeout:    time.Second,
			VerificationTimeout: 5 * time.Second,
		},
		Journal: func(_ context.Context, snapshot supervised.Snapshot[State, Event]) error {
			journal = snapshot
			return nil
		},
	})
	if err != nil {
		panic(err)
	}
	if _, err = run.Start(context.Background(), IO{InterlockClosed: true}); err != nil {
		panic(err)
	}
	fmt.Println("start:", run.Status().Mode)

	issued, err := run.Issue(context.Background(), Move, IO{InterlockClosed: true})
	if err != nil {
		panic(err)
	}
	fmt.Println("issue:", issued.TransitionID, issued.Committed)

	committed, err := run.Verify(context.Background(), issued.AttemptKey, IO{
		InterlockClosed: true,
		AtTarget:        true,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("verify:", run.Snapshot().State, committed.Committed, committed.Revision)
	fmt.Println("journal:", journal.State, journal.InDoubt)

	// Output:
	// start: ready
	// issue: idle-to-work false
	// verify: work true 1
	// journal: work false
}

// ExampleSupervisor_Adjudicate shows the power-loss path: the process dies
// after Issue, the journaled in-doubt Snapshot is restored, and the
// application resolves the Fault with an explicit, evidence-bearing Decision
// instead of guessing.
func ExampleSupervisor_Adjudicate() {
	type State string
	type Event string
	type IO struct{ ControllerAtTarget bool }

	const (
		Idle State = "idle"
		Work State = "work"
		Move Event = "move"
	)

	pass := func(context.Context, supervised.Change[State, Event], IO) error { return nil }
	machine := supervised.MustCompile(supervised.Definition[State, Event, IO]{
		ID:      "arm-position/v1",
		Initial: Idle,
		States: []supervised.State[State, Event]{
			{Name: Idle},
			{Name: Work, Terminal: true},
		},
		Events: []Event{Move},
		Transitions: []supervised.Transition[State, Event, IO]{{
			ID: "idle-to-work", From: Idle, Event: Move, To: Work,
			Issue:  func(context.Context, supervised.Change[State, Event], IO) error { return nil },
			Verify: pass,
		}},
		Preconditions: []supervised.Precondition[State, Event, IO]{
			func(context.Context, supervised.Attempt[State, Event], IO) error { return nil },
		},
		Invariants:     []supervised.Check[State, Event, IO]{pass},
		Postconditions: []supervised.Check[State, Event, IO]{pass},
		Reconcile: []supervised.Reconciler[State, Event, IO]{
			// During adjudication the Reconciler receives the proposed state
			// and must agree it matches physical truth.
			func(_ context.Context, snapshot supervised.Snapshot[State, Event], io IO) error {
				if snapshot.State == Work && !io.ControllerAtTarget {
					return errors.New("proposed state disagrees with controller")
				}
				return nil
			},
		},
	})

	var journal supervised.Snapshot[State, Event]
	options := supervised.Options[State, Event]{
		Limits: supervised.Limits{OperationTimeout: time.Second, VerificationTimeout: 5 * time.Second},
		Journal: func(_ context.Context, snapshot supervised.Snapshot[State, Event]) error {
			journal = snapshot
			return nil
		},
	}
	run, err := supervised.NewWithOptions(machine, options)
	if err != nil {
		panic(err)
	}
	if _, err := run.Start(context.Background(), IO{}); err != nil {
		panic(err)
	}
	if _, err := run.Issue(context.Background(), Move, IO{}); err != nil {
		panic(err)
	}
	// Power loss here: only the journaled in-doubt Snapshot survives.

	restored, err := supervised.RestoreWithOptions(machine, journal, options)
	if err != nil {
		panic(err)
	}
	fmt.Println("restored:", restored.Status().Mode, "restarts:", restored.Status().Restarts)

	// The controller proves the motion completed before the crash, so the
	// in-doubt destination is adopted rather than discarded.
	adopted, err := restored.Adjudicate(context.Background(), supervised.Decision[State]{
		Outcome:  supervised.AdjudicateAdopt,
		Evidence: "controller log: motion 7 acknowledged complete; encoder at target",
	}, IO{ControllerAtTarget: true})
	if err != nil {
		panic(err)
	}
	fmt.Println("adjudicated:", adopted.To, adopted.Committed, restored.Status().Mode)

	// Output:
	// restored: faulted restarts: 1
	// adjudicated: work true ready
}
