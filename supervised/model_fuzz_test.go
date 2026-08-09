package supervised

import (
	"context"
	"testing"
)

// FuzzSupervisorModel exercises arbitrary lifecycle command sequences and
// checks the externally visible mode, pending, fault, and revision model after
// every synchronous operation.
func FuzzSupervisorModel(f *testing.F) {
	f.Add([]byte{0, 1})
	f.Add([]byte{2, 3, 0, 1})
	f.Add([]byte{0, 2, 3, 1, 4})
	f.Fuzz(func(t *testing.T, commands []byte) {
		if len(commands) > 64 {
			commands = commands[:64]
		}
		supervisor, err := New(MustCompile(validDefinition()), limits())
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		_, _ = supervisor.Start(ctx, &testData{})
		var priorRevision uint64

		for _, command := range commands {
			switch command % 5 {
			case 0:
				_, _ = supervisor.Issue(ctx, testStart, &testData{})
			case 1:
				status := supervisor.Status()
				if status.Pending != nil {
					_, _ = supervisor.Verify(ctx, status.Pending.Identifier(), &testData{verified: true})
				} else {
					_, _ = supervisor.Verify(ctx, AttemptID{}, &testData{})
				}
			case 2:
				_ = supervisor.Trip(errTrip)
			case 3:
				_, _ = supervisor.Recover(ctx, &testData{})
			case 4:
				_, _ = supervisor.Start(ctx, &testData{})
			}

			status := supervisor.Status()
			if (status.Mode == ModeAwaitingVerification) != (status.Pending != nil) {
				t.Fatalf("mode/pending disagree after %v: %+v", commands, status)
			}
			if (status.Mode == ModeFaulted) != (status.Fault != nil) {
				t.Fatalf("mode/fault disagree after %v: %+v", commands, status)
			}
			snapshot := supervisor.Snapshot()
			if snapshot.Revision < priorRevision {
				t.Fatalf("revision decreased after %v: %d -> %d", commands, priorRevision, snapshot.Revision)
			}
			priorRevision = snapshot.Revision
		}
	})
}

func benchmarkDefinition(external bool) Definition[testState, testEvent, *testData] {
	definition := Definition[testState, testEvent, *testData]{
		ID:      "benchmark-machine/v1",
		Initial: testIdle,
		States: []State[testState, testEvent]{
			{Name: testIdle, Refuse: []testEvent{testStop}},
			{Name: testRunning, Refuse: []testEvent{testStart}},
		},
		Events: []testEvent{testStart, testStop},
		Transitions: []Transition[testState, testEvent, *testData]{
			{ID: "start", From: testIdle, Event: testStart, To: testRunning},
			{ID: "stop", From: testRunning, Event: testStop, To: testIdle},
		},
		Preconditions:  []Precondition[testState, testEvent, *testData]{passPrecondition},
		Invariants:     []Check[testState, testEvent, *testData]{passCheck},
		Postconditions: []Check[testState, testEvent, *testData]{passCheck},
		Reconcile:      []Reconciler[testState, testEvent, *testData]{passReconcile},
	}
	if external {
		for index := range definition.Transitions {
			definition.Transitions[index].Issue = passAction
			definition.Transitions[index].Verify = passCheck
		}
	}
	return definition
}

func BenchmarkSupervisorLogicalIssue(b *testing.B) {
	benchmarkSupervisor(b, false)
}

func BenchmarkSupervisorExternalIssueVerify(b *testing.B) {
	benchmarkSupervisor(b, true)
}

func benchmarkSupervisor(b *testing.B, external bool) {
	supervisor, err := New(MustCompile(benchmarkDefinition(external)), limits())
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	data := &testData{verified: true}
	if _, err := supervisor.Start(ctx, data); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		event := testStart
		if index%2 == 1 {
			event = testStop
		}
		issued, err := supervisor.Issue(ctx, event, data)
		if err != nil {
			b.Fatal(err)
		}
		if external {
			if _, err := supervisor.Verify(ctx, issued.AttemptKey, data); err != nil {
				b.Fatal(err)
			}
		}
	}
}
