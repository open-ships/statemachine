package supervised

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"
)

type mutableTestError struct{ message string }

func (e *mutableTestError) Error() string { return e.message }

func TestPublicOperationsReturnToolVisibleErrors(t *testing.T) {
	supervisor, _ := New(MustCompile(validDefinition()), limits())
	if result, err := supervisor.Issue(context.Background(), testStart, &testData{}); !errors.Is(err, ErrNotStarted) || result.Err != err {
		t.Fatalf("Issue = %+v, %v", result, err)
	}
	if result, err := supervisor.Start(context.Background(), &testData{}); err != nil || result.Err != nil || result.CompletedAt.IsZero() {
		t.Fatalf("Start = %+v, %v", result, err)
	}
}

func TestFaultResultsNeverExposeSupervisorStorage(t *testing.T) {
	cause := &mutableTestError{message: "original issue failure"}
	definition := validDefinition()
	definition.Transitions[0].Issue = func(context.Context, Change[testState, testEvent], *testData) error {
		return cause
	}
	supervisor, _ := New(MustCompile(definition), limits())
	_ = supervisor.start(context.Background(), &testData{})
	result := supervisor.issue(context.Background(), testStart, &testData{})
	var outward *Fault[testState, testEvent]
	if !errors.As(result.Err, &outward) {
		t.Fatalf("Issue error = %v", result.Err)
	}
	outward.Cause = errors.New("fabricated")
	outward.CauseText = "fabricated"
	outward.Phase = PhaseCommit
	outward.Uncertain = false
	outward.State = testRunning

	status := supervisor.Status()
	if status.Fault == nil || status.Fault.CauseText != "original issue failure" || status.Fault.Phase != PhaseIssue ||
		!status.Fault.Uncertain || status.Fault.State != testIdle || !errors.Is(status.Fault, cause) {
		t.Fatalf("mutable outward Fault changed Status: %+v", status.Fault)
	}
	cause.message = "mutated application error"
	if strings.Contains(supervisor.Status().Fault.Error(), "mutated application error") {
		t.Fatalf("mutable cause changed stable Fault text: %v", supervisor.Status().Fault)
	}
	status.Fault.CauseText = "changed again"
	if next := supervisor.Status().Fault; next.CauseText != "original issue failure" {
		t.Fatalf("mutable Status Fault changed storage: %+v", next)
	}
}

func TestSupervisorOwnsCompiledMachineValue(t *testing.T) {
	original := MustCompile(validDefinition())
	supervisor, _ := New(original, limits())
	replacementDefinition := validDefinition()
	replacementDefinition.ID = "replacement/v1"
	replacementDefinition.Transitions[0].ID = "replacement-transition"
	replacement := MustCompile(replacementDefinition)
	*original = *replacement

	if result := supervisor.start(context.Background(), &testData{}); result.Err != nil {
		t.Fatal(result.Err)
	}
	issued := supervisor.issue(context.Background(), testStart, &testData{})
	if issued.DefinitionID != "test-machine/v1" || issued.TransitionID != "start-running" {
		t.Fatalf("Issue after caller overwrite = %+v", issued)
	}
	if snapshot := supervisor.Snapshot(); snapshot.DefinitionID != "test-machine/v1" {
		t.Fatalf("Snapshot = %+v", snapshot)
	}
}

func TestRestorePreservesAttemptHighWaterAndInDoubtState(t *testing.T) {
	machine := MustCompile(validDefinition())
	original, _ := New(machine, limits())
	_ = original.start(context.Background(), &testData{})
	issued := original.issue(context.Background(), testStart, &testData{})
	snapshot := original.Snapshot()
	if !snapshot.InDoubt || snapshot.Attempt != 1 || snapshot.ExecutionID == "" ||
		snapshot.Pending == nil || snapshot.Pending.Attempt != issued.AttemptKey ||
		snapshot.Pending.TransitionID != "start-running" {
		t.Fatalf("in-doubt Snapshot = %+v", snapshot)
	}
	_ = original.Trip(errTrip)

	restored, err := Restore(machine, snapshot, limits())
	if err != nil {
		t.Fatal(err)
	}
	if status := restored.Status(); status.Mode != ModeFaulted || status.Fault == nil || !status.Fault.Uncertain ||
		status.Fault.ExecutionID != issued.ExecutionID || status.Fault.Event != testStart ||
		status.Fault.TransitionID != "start-running" || status.Fault.To != testRunning || status.Fault.IssuedAt.IsZero() {
		t.Fatalf("restored Status = %+v", status)
	}
	if result := restored.recover(context.Background(), &testData{}); result.Err != nil {
		t.Fatalf("Recover = %+v", result)
	}
	next := restored.issue(context.Background(), testStart, &testData{})
	if next.Attempt != 2 || next.ExecutionID != issued.ExecutionID {
		t.Fatalf("next Attempt = %+v", next)
	}
	if stale := restored.verify(context.Background(), issued.AttemptKey, &testData{}); !errors.Is(stale.Err, ErrStaleAttempt) {
		t.Fatalf("pre-crash Verify = %+v", stale)
	}
	if committed := restored.verify(context.Background(), next.AttemptKey, &testData{}); committed.Err != nil || !committed.Committed {
		t.Fatalf("new Verify = %+v", committed)
	}
}

func TestRecoverWaitsForWholeRevokedOperation(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	definition := validDefinition()
	definition.Transitions[0].Issue = func(context.Context, Change[testState, testEvent], *testData) error {
		close(entered)
		<-release
		return nil
	}
	supervisor, _ := New(MustCompile(definition), limits())
	_ = supervisor.start(context.Background(), &testData{})
	done := make(chan Result[testState, testEvent], 1)
	go func() { done <- supervisor.issue(context.Background(), testStart, &testData{}) }()
	<-entered
	_ = supervisor.Trip(errTrip)
	if result := supervisor.recover(context.Background(), &testData{}); !errors.Is(result.Err, ErrCallbackRunning) {
		t.Fatalf("Recover during revoked operation = %+v", result)
	}
	close(release)
	if result := <-done; !result.Faulted || !errors.Is(result.Err, errTrip) {
		t.Fatalf("Issue after Trip = %+v", result)
	}
	eventually(t, time.Second, func() bool { return !supervisor.Status().OperationRunning })
	if result := supervisor.recover(context.Background(), &testData{}); result.Err != nil || result.Faulted {
		t.Fatalf("Recover after operation return = %+v", result)
	}
}

func TestCallerDeadlineIsNotSupervisorOperationTimeout(t *testing.T) {
	definition := validDefinition()
	definition.Transitions[0].Issue = func(ctx context.Context, _ Change[testState, testEvent], _ *testData) error {
		<-ctx.Done()
		return ctx.Err()
	}
	supervisor, _ := New(MustCompile(definition), Limits{
		OperationTimeout: time.Second, VerificationTimeout: time.Second,
	})
	_ = supervisor.start(context.Background(), &testData{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	result := supervisor.issue(ctx, testStart, &testData{})
	if !errors.Is(result.Err, context.DeadlineExceeded) || errors.Is(result.Err, ErrOperationTimeout) {
		t.Fatalf("Issue error = %v", result.Err)
	}
}

func TestLifecycleRecorderPublishesAsynchronousExpiryAndFailures(t *testing.T) {
	clock := newFakeClock()
	var delivered []Record[testState, testEvent]
	supervisor, err := NewWithOptions(MustCompile(validDefinition()), Options[testState, testEvent]{
		Limits: Limits{OperationTimeout: time.Minute, VerificationTimeout: 10 * time.Second},
		Clock:  clock,
		Recorder: func(_ context.Context, record Record[testState, testEvent]) error {
			delivered = append(delivered, record)
			if record.Kind == RecordIssue {
				return errors.New("recorder offline")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := supervisor.Start(context.Background(), &testData{}); err != nil {
		t.Fatal(err)
	}
	if _, err := supervisor.Issue(context.Background(), testStart, &testData{}); err != nil {
		t.Fatal(err)
	}
	clock.advance(10 * time.Second)
	clock.fireDue()

	records := supervisor.Records()
	if len(records) != 3 || len(delivered) != 3 ||
		records[0].Kind != RecordStart || records[1].Kind != RecordIssue || records[2].Kind != RecordVerificationExpired ||
		records[0].Seq != 1 || records[1].Seq != 2 || records[2].Seq != 3 || records[2].At.IsZero() ||
		!records[2].Faulted || records[2].CauseText == "" {
		t.Fatalf("records = %+v, delivered = %+v", records, delivered)
	}
	if status := supervisor.Status(); status.RecorderError != "recorder offline" || status.Mode != ModeFaulted {
		t.Fatalf("Status = %+v", status)
	}
}

func TestJournalPreparesInDoubtSnapshotBeforeExternalIssue(t *testing.T) {
	prepared := false
	definition := validDefinition()
	definition.Transitions[0].Issue = func(context.Context, Change[testState, testEvent], *testData) error {
		if !prepared {
			return errors.New("Issue ran before durable prepare")
		}
		return nil
	}
	var checkpoint Snapshot[testState, testEvent]
	supervisor, err := NewWithOptions(MustCompile(definition), Options[testState, testEvent]{
		Limits: limits(), RequireJournal: true,
		Journal: func(_ context.Context, snapshot Snapshot[testState, testEvent]) error {
			checkpoint = snapshot
			prepared = true
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = supervisor.Start(context.Background(), &testData{})
	issued, err := supervisor.Issue(context.Background(), testStart, &testData{})
	if err != nil || !issued.IssueCompleted || !checkpoint.InDoubt || checkpoint.Attempt != issued.Attempt ||
		checkpoint.ExecutionID != issued.ExecutionID || checkpoint.RecordedAt.IsZero() || checkpoint.Pending == nil ||
		checkpoint.Pending.Attempt != issued.AttemptKey || checkpoint.Pending.From != testIdle ||
		checkpoint.Pending.Event != testStart || checkpoint.Pending.TransitionID != "start-running" ||
		checkpoint.Pending.To != testRunning || checkpoint.Pending.IssuedAt.IsZero() {
		t.Fatalf("Issue/checkpoint = %+v, %v / %+v", issued, err, checkpoint)
	}
	if _, err := NewWithOptions(MustCompile(validDefinition()), Options[testState, testEvent]{
		Limits: limits(), RequireJournal: true,
	}); !errors.Is(err, ErrJournalRequired) {
		t.Fatalf("missing Journal = %v", err)
	}
}

func TestJournalFailureAndPanicPreventExternalIssue(t *testing.T) {
	for _, test := range []struct {
		name    string
		journal Journal[testState, testEvent]
		want    error
	}{
		{name: "error", journal: func(context.Context, Snapshot[testState, testEvent]) error { return errIssue }, want: errIssue},
		{name: "panic", journal: func(context.Context, Snapshot[testState, testEvent]) error { panic("journal panic") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			issueCalls := 0
			definition := validDefinition()
			definition.Transitions[0].Issue = func(context.Context, Change[testState, testEvent], *testData) error {
				issueCalls++
				return nil
			}
			supervisor, err := NewWithOptions(MustCompile(definition), Options[testState, testEvent]{
				Limits: limits(), Journal: test.journal,
			})
			if err != nil {
				t.Fatal(err)
			}
			_, _ = supervisor.Start(context.Background(), &testData{})
			result, err := supervisor.Issue(context.Background(), testStart, &testData{})
			if issueCalls != 0 || !result.Faulted || !errors.Is(err, ErrJournal) ||
				(test.want != nil && !errors.Is(err, test.want)) {
				t.Fatalf("Issue = %+v, %v; calls %d", result, err, issueCalls)
			}
			if test.name == "panic" {
				var panicError *PanicError
				if !errors.As(err, &panicError) || panicError.Stack == "" {
					t.Fatalf("panic evidence = %v", err)
				}
			}
		})
	}
}

func TestRecorderPanicAndGoexitAreVisibleWithoutChangingOutcome(t *testing.T) {
	for _, test := range []struct {
		name     string
		recorder Recorder[testState, testEvent]
	}{
		{name: "panic", recorder: func(context.Context, Record[testState, testEvent]) error { panic("recorder panic") }},
		{name: "Goexit", recorder: func(context.Context, Record[testState, testEvent]) error { runtime.Goexit(); return nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			supervisor, err := NewWithOptions(MustCompile(validDefinition()), Options[testState, testEvent]{
				Limits: limits(), Recorder: test.recorder,
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := supervisor.Start(context.Background(), &testData{})
			if err != nil || result.Faulted || supervisor.Status().RecorderError == "" || len(supervisor.Records()) != 1 {
				t.Fatalf("Start = %+v, %v; status %+v", result, err, supervisor.Status())
			}
		})
	}
}

func TestRestoreClockAndOptionsSeams(t *testing.T) {
	machine := MustCompile(validDefinition())
	original, _ := New(machine, limits())
	snapshot := original.Snapshot()
	clock := newFakeClock()
	withClock, err := RestoreWithClock(machine, snapshot, limits(), clock)
	if err != nil || withClock.Snapshot().RecordedAt != clock.Now() {
		t.Fatalf("RestoreWithClock = %v, %+v", err, withClock)
	}
	var records []Record[testState, testEvent]
	withOptions, err := RestoreWithOptions(machine, snapshot, Options[testState, testEvent]{
		Limits: limits(), Clock: clock,
		Recorder: func(_ context.Context, record Record[testState, testEvent]) error {
			records = append(records, record)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := withOptions.Start(context.Background(), &testData{}); err != nil ||
		len(records) != 1 || records[0].Kind != RecordStart || records[0].At != clock.Now() {
		t.Fatalf("restored Start = %v, records %+v", err, records)
	}
}
