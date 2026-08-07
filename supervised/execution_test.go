package supervised

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"
)

var (
	errPrecondition = errors.New("unsafe input")
	errIssue        = errors.New("issue failed")
	errVerify       = errors.New("physical state not verified")
	errReconcile    = errors.New("state did not reconcile")
	errTrip         = errors.New("protective trip")
)

func limits() Limits {
	return Limits{OperationTimeout: time.Second, VerificationTimeout: time.Second}
}

func tracedDefinition() Definition[testState, testEvent, *testData] {
	definition := validDefinition()
	definition.Preconditions = []Precondition[testState, testEvent, *testData]{
		func(_ context.Context, _ Attempt[testState, testEvent], data *testData) error {
			data.trace = append(data.trace, "precondition")
			return nil
		},
	}
	definition.Invariants = []Check[testState, testEvent, *testData]{
		func(_ context.Context, _ Change[testState, testEvent], data *testData) error {
			data.trace = append(data.trace, "invariant")
			return nil
		},
	}
	definition.Postconditions = []Check[testState, testEvent, *testData]{
		func(_ context.Context, _ Change[testState, testEvent], data *testData) error {
			data.trace = append(data.trace, "postcondition")
			return nil
		},
	}
	definition.Reconcile = []Reconciler[testState, *testData]{
		func(_ context.Context, _ Snapshot[testState], data *testData) error {
			data.trace = append(data.trace, "reconcile")
			return nil
		},
	}
	definition.Transitions[0].Guard = func(_ context.Context, _ Change[testState, testEvent], data *testData) error {
		data.trace = append(data.trace, "guard")
		return nil
	}
	definition.Transitions[0].Preconditions = []Check[testState, testEvent, *testData]{
		func(_ context.Context, _ Change[testState, testEvent], data *testData) error {
			data.trace = append(data.trace, "transition precondition")
			return nil
		},
	}
	definition.Transitions[0].Issue = func(_ context.Context, _ Change[testState, testEvent], data *testData) error {
		data.trace = append(data.trace, "issue")
		return nil
	}
	definition.Transitions[0].Verify = func(_ context.Context, _ Change[testState, testEvent], data *testData) error {
		data.trace = append(data.trace, "verify")
		if !data.verified {
			return errVerify
		}
		return nil
	}
	return definition
}

func TestIssueVerifyCommitAndSnapshot(t *testing.T) {
	machine := MustCompile(tracedDefinition())
	supervisor, err := New(machine, limits())
	if err != nil {
		t.Fatal(err)
	}
	data := &testData{}

	if result := supervisor.Issue(context.Background(), testStart, data); !errors.Is(result.Err, ErrNotStarted) {
		t.Fatalf("Issue before Start = %+v", result)
	}
	if result := supervisor.Start(context.Background(), data); result.Err != nil || result.Phase != PhaseStart {
		t.Fatalf("Start = %+v", result)
	}
	issued := supervisor.Issue(context.Background(), testStart, data)
	if issued.Err != nil || !issued.Selected || !issued.IssueCompleted || issued.Verified || issued.Committed || issued.Attempt != 1 {
		t.Fatalf("Issue = %+v", issued)
	}
	if snapshot := supervisor.Snapshot(); snapshot.State != testIdle || snapshot.Revision != 0 {
		t.Fatalf("Snapshot after Issue = %+v", snapshot)
	}
	status := supervisor.Status()
	if status.Mode != ModeAwaitingVerification || status.Pending == nil || status.Pending.ID != issued.Attempt {
		t.Fatalf("Status after Issue = %+v", status)
	}

	stale := supervisor.Verify(context.Background(), issued.Attempt+1, &testData{verified: true})
	if !errors.Is(stale.Err, ErrStaleAttempt) || supervisor.Status().Mode != ModeAwaitingVerification {
		t.Fatalf("stale Verify = %+v, status %+v", stale, supervisor.Status())
	}
	verifiedData := &testData{trace: data.trace, verified: true}
	committed := supervisor.Verify(context.Background(), issued.Attempt, verifiedData)
	if committed.Err != nil || !committed.IssueCompleted || !committed.Verified || !committed.Committed || committed.Revision != 1 {
		t.Fatalf("Verify = %+v", committed)
	}
	if snapshot := supervisor.Snapshot(); snapshot != (Snapshot[testState]{DefinitionID: "test-machine/v1", State: testRunning, Revision: 1}) {
		t.Fatalf("Snapshot after Verify = %+v", snapshot)
	}
	wantTrace := []string{
		"reconcile", "precondition", "guard", "transition precondition", "invariant", "issue",
		"verify", "invariant", "postcondition",
	}
	if strings.Join(verifiedData.trace, ",") != strings.Join(wantTrace, ",") {
		t.Fatalf("trace = %v, want %v", verifiedData.trace, wantTrace)
	}
	if result := supervisor.Verify(context.Background(), issued.Attempt, verifiedData); !errors.Is(result.Err, ErrNoPending) {
		t.Fatalf("second Verify = %+v", result)
	}
	if result := supervisor.Issue(context.Background(), testStart, verifiedData); !errors.Is(result.Err, ErrNotPermitted) || result.Faulted {
		t.Fatalf("terminal Issue = %+v", result)
	}
}

func TestPureLogicalTransitionCommitsDuringIssue(t *testing.T) {
	definition := tracedDefinition()
	definition.Transitions[0].Issue = nil
	definition.Transitions[0].Verify = nil
	machine := MustCompile(definition)
	supervisor, _ := New(machine, limits())
	data := &testData{}
	if result := supervisor.Start(context.Background(), data); result.Err != nil {
		t.Fatal(result.Err)
	}
	result := supervisor.Issue(context.Background(), testStart, data)
	if result.Err != nil || !result.Selected || result.IssueCompleted || result.Verified || !result.Committed || result.Revision != 1 {
		t.Fatalf("Issue = %+v", result)
	}
	want := "reconcile,precondition,guard,transition precondition,invariant,postcondition"
	if got := strings.Join(data.trace, ","); got != want {
		t.Fatalf("trace = %q, want %q", got, want)
	}
}

func TestPreconditionsCannotFallThroughAndGuardRefusalDoesNotFault(t *testing.T) {
	definition := validDefinition()
	definition.States = append(definition.States, State[testState, testEvent]{Name: testOther, Terminal: true})
	definition.Transitions = []Transition[testState, testEvent, *testData]{
		{
			ID: "guarded", From: testIdle, Event: testStart, To: testRunning,
			Guard: func(context.Context, Change[testState, testEvent], *testData) error { return errVerify },
			Issue: passAction, Verify: passCheck,
		},
		{
			ID: "fallback", From: testIdle, Event: testStart, To: testOther,
			Preconditions: []Check[testState, testEvent, *testData]{
				func(context.Context, Change[testState, testEvent], *testData) error { return errPrecondition },
			},
			Issue: passAction, Verify: passCheck,
		},
	}
	machine := MustCompile(definition)
	supervisor, _ := New(machine, limits())
	if result := supervisor.Start(context.Background(), &testData{}); result.Err != nil {
		t.Fatal(result.Err)
	}
	result := supervisor.Issue(context.Background(), testStart, &testData{})
	if !result.Selected || result.TransitionID != "fallback" || !result.Faulted || !errors.Is(result.Err, errPrecondition) || !errors.Is(result.Err, ErrViolation) {
		t.Fatalf("Issue = %+v", result)
	}

	guardOnly := validDefinition()
	guardOnly.Transitions[0].Guard = func(context.Context, Change[testState, testEvent], *testData) error { return errVerify }
	machine = MustCompile(guardOnly)
	supervisor, _ = New(machine, limits())
	_ = supervisor.Start(context.Background(), &testData{})
	result = supervisor.Issue(context.Background(), testStart, &testData{})
	if !errors.Is(result.Err, ErrNotPermitted) || !errors.Is(result.Err, errVerify) || result.Faulted || supervisor.Status().Mode != ModeReady {
		t.Fatalf("guard refusal = %+v, status %+v", result, supervisor.Status())
	}
}

func TestIssueFailureLatchesUntilSuccessfulRecovery(t *testing.T) {
	definition := validDefinition()
	definition.Transitions[0].Issue = func(context.Context, Change[testState, testEvent], *testData) error {
		return errIssue
	}
	definition.Reconcile = []Reconciler[testState, *testData]{
		func(_ context.Context, _ Snapshot[testState], data *testData) error {
			if !data.allow {
				return errReconcile
			}
			return nil
		},
	}
	machine := MustCompile(definition)
	supervisor, _ := New(machine, limits())
	if result := supervisor.Start(context.Background(), &testData{allow: true}); result.Err != nil {
		t.Fatal(result.Err)
	}
	failed := supervisor.Issue(context.Background(), testStart, &testData{})
	if !failed.Faulted || !failed.Uncertain || failed.Committed || !errors.Is(failed.Err, ErrFaulted) || !errors.Is(failed.Err, errIssue) {
		t.Fatalf("failed Issue = %+v", failed)
	}
	if snapshot := supervisor.Snapshot(); snapshot.State != testIdle || snapshot.Revision != 0 {
		t.Fatalf("Snapshot = %+v", snapshot)
	}
	if again := supervisor.Issue(context.Background(), testStart, &testData{}); !errors.Is(again.Err, errIssue) {
		t.Fatalf("Issue while faulted = %+v", again)
	}
	if recovery := supervisor.Recover(context.Background(), &testData{}); !recovery.Faulted || !errors.Is(recovery.Err, errReconcile) {
		t.Fatalf("failed Recover = %+v", recovery)
	}
	if recovery := supervisor.Recover(context.Background(), &testData{allow: true}); recovery.Err != nil || recovery.Faulted {
		t.Fatalf("Recover = %+v", recovery)
	}
	if supervisor.Status().Mode != ModeReady {
		t.Fatalf("Status = %+v", supervisor.Status())
	}
}

func TestVerifyFailureIsUncertainAndDoesNotCommit(t *testing.T) {
	machine := MustCompile(tracedDefinition())
	supervisor, _ := New(machine, limits())
	data := &testData{}
	_ = supervisor.Start(context.Background(), data)
	issued := supervisor.Issue(context.Background(), testStart, data)
	result := supervisor.Verify(context.Background(), issued.Attempt, &testData{})
	if !result.IssueCompleted || result.Verified || result.Committed || !result.Faulted || !result.Uncertain || !errors.Is(result.Err, errVerify) {
		t.Fatalf("Verify = %+v", result)
	}
	if supervisor.Snapshot().State != testIdle {
		t.Fatalf("Snapshot = %+v", supervisor.Snapshot())
	}
}

func TestCallbackPanicAndGoexitAreContained(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*Definition[testState, testEvent, *testData])
		want      error
		uncertain bool
	}{
		{
			name: "panic in issue",
			configure: func(definition *Definition[testState, testEvent, *testData]) {
				definition.Transitions[0].Issue = func(context.Context, Change[testState, testEvent], *testData) error {
					panic("motor panic")
				}
			},
			uncertain: true,
		},
		{
			name: "Goexit in guard",
			configure: func(definition *Definition[testState, testEvent, *testData]) {
				definition.Transitions[0].Guard = func(context.Context, Change[testState, testEvent], *testData) error {
					runtime.Goexit()
					return nil
				}
			},
			want: ErrExecutionStopped,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := validDefinition()
			test.configure(&definition)
			supervisor, _ := New(MustCompile(definition), limits())
			_ = supervisor.Start(context.Background(), &testData{})
			result := supervisor.Issue(context.Background(), testStart, &testData{})
			if !result.Faulted || result.Uncertain != test.uncertain {
				t.Fatalf("Issue = %+v", result)
			}
			if test.want != nil && !errors.Is(result.Err, test.want) {
				t.Fatalf("Issue error = %v, want %v", result.Err, test.want)
			}
			if test.want == nil {
				var panicErr *PanicError
				if !errors.As(result.Err, &panicErr) || panicErr.Value != "motor panic" {
					t.Fatalf("Issue error = %v", result.Err)
				}
			}
		})
	}
}

func TestOperationTimeoutLeavesLateCallbackUncommittedAndBlocksRecovery(t *testing.T) {
	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	definition := validDefinition()
	definition.Transitions[0].Issue = func(context.Context, Change[testState, testEvent], *testData) error {
		entered <- struct{}{}
		<-block // intentionally ignores cancellation
		return nil
	}
	machine := MustCompile(definition)
	supervisor, _ := New(machine, Limits{
		OperationTimeout:    20 * time.Millisecond,
		VerificationTimeout: time.Second,
	})
	_ = supervisor.Start(context.Background(), &testData{})
	started := time.Now()
	result := supervisor.Issue(context.Background(), testStart, &testData{})
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("Issue returned after %v", elapsed)
	}
	if !result.Faulted || !result.Uncertain || !errors.Is(result.Err, ErrOperationTimeout) {
		t.Fatalf("Issue = %+v", result)
	}
	select {
	case <-entered:
	default:
		t.Fatal("Issue callback did not start")
	}
	if status := supervisor.Status(); !status.CallbackRunning {
		t.Fatalf("Status = %+v", status)
	}
	if recovery := supervisor.Recover(context.Background(), &testData{}); !errors.Is(recovery.Err, ErrCallbackRunning) {
		t.Fatalf("Recover = %+v", recovery)
	}
	close(block)
	eventually(t, time.Second, func() bool { return !supervisor.Status().CallbackRunning })
	if recovery := supervisor.Recover(context.Background(), &testData{}); recovery.Err != nil {
		t.Fatalf("Recover after callback return = %+v", recovery)
	}
	if snapshot := supervisor.Snapshot(); snapshot.State != testIdle || snapshot.Revision != 0 {
		t.Fatalf("late callback committed: %+v", snapshot)
	}
}

func TestVerificationTimeoutLatchesFault(t *testing.T) {
	machine := MustCompile(validDefinition())
	supervisor, _ := New(machine, Limits{
		OperationTimeout:    time.Second,
		VerificationTimeout: 20 * time.Millisecond,
	})
	_ = supervisor.Start(context.Background(), &testData{})
	issued := supervisor.Issue(context.Background(), testStart, &testData{})
	if issued.Err != nil || !issued.IssueCompleted {
		t.Fatalf("Issue = %+v", issued)
	}
	eventually(t, time.Second, func() bool { return supervisor.Status().Mode == ModeFaulted })
	status := supervisor.Status()
	if status.Fault == nil || !errors.Is(status.Fault, ErrVerificationTimeout) || !status.Fault.Uncertain {
		t.Fatalf("Status = %+v", status)
	}
	if supervisor.Snapshot().State != testIdle {
		t.Fatalf("Snapshot = %+v", supervisor.Snapshot())
	}
}

func TestTripRacingVerifyPreventsCommitAndIsFirstCause(t *testing.T) {
	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	definition := validDefinition()
	definition.Transitions[0].Verify = func(context.Context, Change[testState, testEvent], *testData) error {
		entered <- struct{}{}
		<-block
		return nil
	}
	supervisor, _ := New(MustCompile(definition), limits())
	_ = supervisor.Start(context.Background(), &testData{})
	issued := supervisor.Issue(context.Background(), testStart, &testData{})
	resultCh := make(chan Result[testState, testEvent], 1)
	go func() {
		resultCh <- supervisor.Verify(context.Background(), issued.Attempt, &testData{})
	}()
	<-entered
	fault := supervisor.Trip(errTrip)
	if !errors.Is(&fault, errTrip) || !fault.Uncertain || fault.Phase != PhaseVerify {
		t.Fatalf("Trip = %+v", fault)
	}
	second := supervisor.Trip(errors.New("later"))
	if !errors.Is(&second, errTrip) {
		t.Fatalf("second Trip replaced first: %+v", second)
	}
	result := <-resultCh
	if !result.Faulted || result.Committed || !errors.Is(result.Err, errTrip) {
		t.Fatalf("Verify = %+v", result)
	}
	close(block)
	eventually(t, time.Second, func() bool { return !supervisor.Status().CallbackRunning })
	if snapshot := supervisor.Snapshot(); snapshot.State != testIdle || snapshot.Revision != 0 {
		t.Fatalf("Snapshot = %+v", snapshot)
	}
}

func TestRestoreRequiresMatchingDefinitionAndStartupReconciliation(t *testing.T) {
	machine := MustCompile(validDefinition())
	if _, err := Restore(machine, Snapshot[testState]{DefinitionID: "other", State: testIdle}, limits()); !errors.Is(err, ErrDefinitionMismatch) {
		t.Fatalf("definition mismatch = %v", err)
	}
	if _, err := Restore(machine, Snapshot[testState]{DefinitionID: machine.ID(), State: testOther}, limits()); !errors.Is(err, ErrUnknownState) {
		t.Fatalf("unknown state = %v", err)
	}
	supervisor, err := Restore(machine, Snapshot[testState]{DefinitionID: machine.ID(), State: testRunning, Revision: 9}, limits())
	if err != nil {
		t.Fatal(err)
	}
	if supervisor.Status().Mode != ModeStopped {
		t.Fatalf("Status = %+v", supervisor.Status())
	}
	if result := supervisor.Issue(context.Background(), testStart, &testData{}); !errors.Is(result.Err, ErrNotStarted) {
		t.Fatalf("Issue = %+v", result)
	}
	if result := supervisor.Start(context.Background(), &testData{}); result.Err != nil {
		t.Fatalf("Start = %+v", result)
	}
	if snapshot := supervisor.Snapshot(); snapshot.State != testRunning || snapshot.Revision != 9 {
		t.Fatalf("Snapshot = %+v", snapshot)
	}
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not satisfied before timeout")
		}
		runtime.Gosched()
	}
}
