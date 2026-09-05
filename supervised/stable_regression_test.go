package supervised

import (
	"context"
	"errors"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func stableAwait[V any](t *testing.T, done <-chan V) V {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(2 * time.Second):
		t.Fatal("public operation did not return")
		var zero V
		return zero
	}
}

func stableWaitIdle(t *testing.T, supervisor *Supervisor[testState, testEvent, *testData]) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		status := supervisor.Status()
		if !status.OperationRunning && !status.CallbackRunning && !status.RecorderRunning && !status.JournalRunning {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("callback ownership did not end: %+v", status)
		}
		runtime.Gosched()
	}
}

func TestStableLateJournalRetainsOwnership(t *testing.T) {
	for _, phase := range []string{"startup closure", "issue preparation"} {
		t.Run(phase, func(t *testing.T) {
			clock := newFakeClock()
			entered := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			var blocked atomic.Bool
			var issued atomic.Int64
			definition := validDefinition()
			definition.Transitions[0].Issue = func(context.Context, Change[testState, testEvent], *testData) error {
				issued.Add(1)
				return nil
			}
			supervisor, err := NewWithOptions(MustCompile(definition), Options[testState, testEvent]{
				Limits:          Limits{OperationTimeout: time.Minute, VerificationTimeout: time.Minute},
				Clock:           clock,
				RecorderTimeout: 20 * time.Millisecond,
				Journal: func(_ context.Context, snapshot Snapshot[testState, testEvent]) error {
					if (phase == "startup closure" || snapshot.InDoubt) && blocked.CompareAndSwap(false, true) {
						close(entered)
						<-release // deliberately outlive the caller's context
					}
					return nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan Result[testState, testEvent], 1)
			go func() {
				result, err := supervisor.Start(context.Background(), &testData{})
				if phase == "issue preparation" && err == nil {
					result, _ = supervisor.Issue(context.Background(), testStart, &testData{})
				}
				done <- result
			}()
			stableAwait(t, entered)
			if phase == "issue preparation" {
				clock.advance(time.Minute)
				clock.fireDue()
			}
			result := stableAwait(t, done)
			if phase == "issue preparation" && (!errors.Is(result.Err, ErrJournal) || !result.Faulted) {
				t.Fatalf("preparation timeout = %+v", result)
			}
			status := supervisor.Status()
			if !status.JournalRunning || !status.OperationRunning || issued.Load() != 0 {
				t.Fatalf("late Journal ownership = %+v; Issue calls = %d", status, issued.Load())
			}
			if _, err := supervisor.Issue(context.Background(), testStart, &testData{}); err == nil {
				t.Fatal("new Issue was admitted while the old Journal still ran")
			}
			supervisor.Trip(errors.New("reconcile the stale write"))
			if _, err := supervisor.Recover(context.Background(), &testData{}); !errors.Is(err, ErrCallbackRunning) {
				t.Fatalf("Recover while Journal runs = %v", err)
			}
			releaseOnce.Do(func() { close(release) })
			stableWaitIdle(t, supervisor)
			if _, err := supervisor.Recover(context.Background(), &testData{}); err != nil {
				t.Fatalf("Recover after Journal stopped = %v", err)
			}
			if issued.Load() != 0 {
				t.Fatal("recovery replayed the refused external Issue")
			}
		})
	}
}

func TestStableRepeatedRestoreUsesFreshAttemptAndRecordIdentities(t *testing.T) {
	machine := MustCompile(validDefinition())
	original, err := newUnjournaled(machine, limits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := original.Start(context.Background(), &testData{}); err != nil {
		t.Fatal(err)
	}
	snapshot := original.Snapshot()
	var restored [2]*Supervisor[testState, testEvent, *testData]
	var attempts [2]Result[testState, testEvent]
	for index := range restored {
		restored[index], err = RestoreWithOptions(machine, snapshot, Options[testState, testEvent]{Limits: limits(), Unjournaled: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := restored[index].Start(context.Background(), &testData{}); err != nil {
			t.Fatal(err)
		}
		attempts[index], err = restored[index].Issue(context.Background(), testStart, &testData{})
		if err != nil {
			t.Fatal(err)
		}
	}
	left, right := restored[0].Snapshot(), restored[1].Snapshot()
	if left.ExecutionID != snapshot.ExecutionID || right.ExecutionID != snapshot.ExecutionID || left.Restarts != right.Restarts ||
		left.IncarnationID == "" || right.IncarnationID == "" || left.IncarnationID == right.IncarnationID ||
		left.IncarnationID == snapshot.IncarnationID || right.IncarnationID == snapshot.IncarnationID {
		t.Fatalf("restored identities = %+v / %+v", left, right)
	}
	if attempts[0].Attempt != attempts[1].Attempt || attempts[0].AttemptKey == attempts[1].AttemptKey {
		t.Fatalf("attempt identities were reused: %+v / %+v", attempts[0].AttemptKey, attempts[1].AttemptKey)
	}
	for _, first := range restored[0].Records() {
		for _, second := range restored[1].Records() {
			if first.ExecutionID == second.ExecutionID && first.IncarnationID == second.IncarnationID && first.Seq == second.Seq {
				t.Fatalf("Record identity reused: %+v / %+v", first, second)
			}
		}
	}
	if _, err := restored[0].Verify(context.Background(), attempts[1].AttemptKey, &testData{}); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("other incarnation's attempt = %v", err)
	}
	for index, supervisor := range restored {
		if result, err := supervisor.Verify(context.Background(), attempts[index].AttemptKey, &testData{}); err != nil || !result.Committed {
			t.Fatalf("own incarnation Verify = %+v, %v", result, err)
		}
	}
}

func TestStableSameStateAdjudicationAdvancesRevision(t *testing.T) {
	for _, outcome := range []Adjudication{AdjudicateAdopt, AdjudicateOverride, AdjudicateRetain} {
		t.Run(outcome.String(), func(t *testing.T) {
			definition := validDefinition()
			definition.States = []State[testState, testEvent]{{Name: testIdle}}
			definition.Transitions[0].To = testIdle
			supervisor, err := newUnjournaled(MustCompile(definition), limits())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := supervisor.Start(context.Background(), &testData{}); err != nil {
				t.Fatal(err)
			}
			if outcome == AdjudicateAdopt {
				if _, err := supervisor.Issue(context.Background(), testStart, &testData{}); err != nil {
					t.Fatal(err)
				}
			}
			supervisor.Trip(errors.New("external condition reconciled"))
			result, err := supervisor.Adjudicate(context.Background(), Decision[testState]{Outcome: outcome, State: testIdle, Evidence: "application established idle"}, &testData{})
			if err != nil || supervisor.Snapshot().State != testIdle {
				t.Fatalf("same-state adjudication = %+v, %v", result, err)
			}
			wantRevision := uint64(1)
			if outcome == AdjudicateRetain {
				wantRevision = 0
			}
			if result.Revision != wantRevision || supervisor.Snapshot().Revision != wantRevision || result.Committed != (outcome != AdjudicateRetain) {
				t.Fatalf("same-state adjudication revision = %+v, snapshot %+v", result, supervisor.Snapshot())
			}
		})
	}
}

func TestStableRecordKeepsOriginalChangeAndSeparateOutcomeTiming(t *testing.T) {
	clock := newFakeClock()
	definition := validDefinition()
	var issuedChange, verifiedChange Change[testState, testEvent]
	definition.Transitions[0].Issue = func(_ context.Context, change Change[testState, testEvent], _ *testData) error {
		issuedChange = change
		clock.advance(time.Second)
		return nil
	}
	definition.Transitions[0].Verify = func(_ context.Context, change Change[testState, testEvent], _ *testData) error {
		verifiedChange = change
		clock.advance(2 * time.Second)
		return nil
	}
	supervisor, err := newUnjournaledWithClock(MustCompile(definition), Limits{OperationTimeout: time.Minute, VerificationTimeout: time.Minute}, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := supervisor.Start(context.Background(), &testData{}); err != nil {
		t.Fatal(err)
	}
	issued, err := supervisor.Issue(context.Background(), testStart, &testData{})
	if err != nil {
		t.Fatal(err)
	}
	clock.advance(5 * time.Second)
	verifyStarted := clock.Now()
	verified, err := supervisor.Verify(context.Background(), issued.AttemptKey, &testData{})
	if err != nil || !verified.Committed || issued.Change != issuedChange || verified.Change != issuedChange || verifiedChange != issuedChange {
		t.Fatalf("command identity changed: issued %+v, callback %+v, verified %+v, verify callback %+v, err %v", issued.Change, issuedChange, verified.Change, verifiedChange, err)
	}
	var found bool
	for _, record := range supervisor.Records() {
		if record.Kind != RecordVerify {
			continue
		}
		found = true
		if record.Change != issuedChange || record.Change.Revision != 0 || record.Revision != 1 ||
			record.OperationStartedAt != verifyStarted || record.OperationCompletedAt != verifyStarted.Add(2*time.Second) ||
			record.Change.StartedAt == record.OperationStartedAt || record.At != record.OperationCompletedAt {
			t.Fatalf("Record mixes command and outcome evidence: %+v; original %+v", record, issuedChange)
		}
	}
	if !found {
		t.Fatal("missing Verify Record")
	}
}

func TestStableTimedOutRecorderBlocksRecoveryUntilItStops(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var calls atomic.Int64
	supervisor, err := NewWithOptions(MustCompile(validDefinition()), Options[testState, testEvent]{
		Limits: limits(), Unjournaled: true, RecorderTimeout: 20 * time.Millisecond,
		Recorder: func(context.Context, Record[testState, testEvent]) error {
			if calls.Add(1) == 1 {
				<-release
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
	if status := supervisor.Status(); !status.RecorderRunning || !status.OperationRunning || status.RecorderError == "" {
		t.Fatalf("timed-out Recorder status = %+v", status)
	}
	supervisor.Trip(errors.New("reconcile while recorder is late"))
	if _, err := supervisor.Recover(context.Background(), &testData{}); !errors.Is(err, ErrCallbackRunning) {
		t.Fatalf("Recover while Recorder runs = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("Recorder deliveries queued behind the blocked one: %d", calls.Load())
	}
	releaseOnce.Do(func() { close(release) })
	stableWaitIdle(t, supervisor)
	if _, err := supervisor.Recover(context.Background(), &testData{}); err != nil {
		t.Fatalf("Recover after Recorder stopped = %v", err)
	}
}

func TestStableRecorderCanTripWithoutWaitingOnItself(t *testing.T) {
	tripped := make(chan Fault[testState, testEvent], 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var supervisor *Supervisor[testState, testEvent, *testData]
	var calls atomic.Int64
	var err error
	supervisor, err = NewWithOptions(MustCompile(validDefinition()), Options[testState, testEvent]{
		Limits: limits(), Unjournaled: true, RecorderTimeout: 5 * time.Second,
		Recorder: func(context.Context, Record[testState, testEvent]) error {
			if calls.Add(1) == 1 {
				tripped <- supervisor.Trip(errors.New("recorder requests trip"))
				<-release
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan Result[testState, testEvent], 1)
	go func() {
		result, _ := supervisor.Start(context.Background(), &testData{})
		done <- result
	}()
	// The enclosing Recorder has a five-second budget. Its nested Trip must
	// complete without consuming that budget or enqueueing another delivery.
	_ = stableAwait(t, tripped)
	if calls.Load() != 1 || supervisor.Status().Mode != ModeFaulted {
		t.Fatalf("nested Trip deliveries/mode = %d/%v", calls.Load(), supervisor.Status().Mode)
	}
	if _, err := supervisor.Recover(context.Background(), &testData{}); !errors.Is(err, ErrCallbackRunning) {
		t.Fatalf("Recover from live recorder = %v", err)
	}
	releaseOnce.Do(func() { close(release) })
	_ = stableAwait(t, done)
	stableWaitIdle(t, supervisor)
}

type stableDelayedError struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (e *stableDelayedError) Error() string {
	e.once.Do(func() { close(e.entered) })
	<-e.release
	return "delayed refusal formatting"
}

func TestStableRetentionKeepsNewestCausalRecordsWhenPublicationIsLate(t *testing.T) {
	failure := &stableDelayedError{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(failure.release) }) })
	definition := validDefinition()
	definition.Transitions[0].Guard = func(context.Context, Change[testState, testEvent], *testData) error { return failure }
	supervisor, err := NewWithOptions(MustCompile(definition), Options[testState, testEvent]{
		Limits: Limits{OperationTimeout: time.Second, VerificationTimeout: time.Second, MaxRecords: 3}, Unjournaled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := supervisor.Start(context.Background(), &testData{}); err != nil {
		t.Fatal(err)
	}
	done := make(chan Result[testState, testEvent], 1)
	go func() {
		result, _ := supervisor.Issue(context.Background(), testStart, &testData{})
		done <- result
	}()
	stableAwait(t, failure.entered) // Seq 2 was decided, but is not published yet.
	for range 3 {
		if _, err := supervisor.Issue(context.Background(), testStart, &testData{}); !errors.Is(err, ErrBusy) {
			t.Fatalf("overlapping Issue = %v", err)
		}
	}
	releaseOnce.Do(func() { close(failure.release) })
	if result := stableAwait(t, done); !errors.Is(result.Err, ErrNotPermitted) {
		t.Fatalf("delayed Issue = %+v", result)
	}
	records := supervisor.Records()
	if len(records) != 3 {
		t.Fatalf("retained records = %+v", records)
	}
	for index, record := range records {
		if record.Seq != uint64(index+3) {
			t.Fatalf("late Seq 2 displaced a newer Record: %+v", records)
		}
	}
	if status := supervisor.Status(); status.Snapshot.Records != 5 || status.RecordsDropped != 2 {
		t.Fatalf("retention accounting = %+v", status)
	}
}

func TestStableCounterExhaustionCannotReuseIdentity(t *testing.T) {
	machine := MustCompile(validDefinition())
	seed, err := newUnjournaled(machine, limits())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := seed.Snapshot()
	for _, counter := range []string{"revision", "restarts", "records"} {
		t.Run("restore "+counter, func(t *testing.T) {
			invalid := snapshot
			switch counter {
			case "revision":
				invalid.Revision = math.MaxUint64
			case "restarts":
				invalid.Restarts = math.MaxUint64
			case "records":
				invalid.Records = math.MaxUint64
			}
			if _, err := restoreUnjournaled(machine, invalid, limits()); !errors.Is(err, ErrCounterExhausted) {
				t.Fatalf("exhausted restore = %v", err)
			}
		})
	}
	t.Run("attempt", func(t *testing.T) {
		exhausted := snapshot
		exhausted.Attempt = math.MaxUint64
		supervisor, err := restoreUnjournaled(machine, exhausted, limits())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := supervisor.Start(context.Background(), &testData{}); err != nil {
			t.Fatal(err)
		}
		result, err := supervisor.Issue(context.Background(), testStart, &testData{})
		if !errors.Is(err, ErrCounterExhausted) || !result.Faulted || result.IssueCompleted || supervisor.Snapshot().Attempt != math.MaxUint64 {
			t.Fatalf("exhausted Attempt = %+v, %v; snapshot %+v", result, err, supervisor.Snapshot())
		}
	})
	t.Run("record sequence", func(t *testing.T) {
		near := snapshot
		near.Records = math.MaxUint64 - 128
		supervisor, err := RestoreWithOptions(machine, near, Options[testState, testEvent]{
			Limits: Limits{OperationTimeout: time.Second, VerificationTimeout: time.Second, MaxRecords: 4}, Unjournaled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := supervisor.Start(context.Background(), &testData{}); err != nil {
			t.Fatal(err)
		}
		for range 200 {
			_, _ = supervisor.Issue(context.Background(), testStop, &testData{})
		}
		if status := supervisor.Status(); status.Snapshot.Records != math.MaxUint64 || !status.RecordsExhausted {
			t.Fatalf("record high-water wrapped or exhaustion hidden: %+v", status)
		}
		for _, record := range supervisor.Records() {
			if record.Seq <= near.Records {
				t.Fatalf("record identity wrapped: %+v", record)
			}
		}
		if _, err := supervisor.Issue(context.Background(), testStart, &testData{}); !errors.Is(err, ErrCounterExhausted) {
			t.Fatalf("new work after Record exhaustion = %v", err)
		}
	})
}
