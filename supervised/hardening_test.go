package supervised

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// journalLog retains every Journal write in order for exact assertions.
type journalLog struct {
	mu     sync.Mutex
	writes []Snapshot[testState, testEvent]
	fail   func(Snapshot[testState, testEvent]) error
}

func (j *journalLog) journal() Journal[testState, testEvent] {
	return func(_ context.Context, snapshot Snapshot[testState, testEvent]) error {
		j.mu.Lock()
		defer j.mu.Unlock()
		if j.fail != nil {
			if err := j.fail(snapshot); err != nil {
				return err
			}
		}
		j.writes = append(j.writes, snapshot)
		return nil
	}
}

func (j *journalLog) all() []Snapshot[testState, testEvent] {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]Snapshot[testState, testEvent](nil), j.writes...)
}

func TestConstructionRequiresJournalForExternalMachines(t *testing.T) {
	external := MustCompile(validDefinition())
	logical := validDefinition()
	logical.Transitions[0].Issue = nil
	logical.Transitions[0].Verify = nil
	logicalMachine := MustCompile(logical)

	if !external.External() || logicalMachine.External() {
		t.Fatalf("External() = %v/%v", external.External(), logicalMachine.External())
	}
	if _, err := New(external, limits()); !errors.Is(err, ErrJournalRequired) {
		t.Fatalf("New external = %v", err)
	}
	if _, err := NewWithClock(external, limits(), newFakeClock()); !errors.Is(err, ErrJournalRequired) {
		t.Fatalf("NewWithClock external = %v", err)
	}
	snapshot := Snapshot[testState, testEvent]{
		Version: SnapshotVersion, DefinitionID: external.ID(), ExecutionID: "execution", IncarnationID: "stored-incarnation", State: testIdle,
	}
	if _, err := Restore(external, snapshot, limits()); !errors.Is(err, ErrJournalRequired) {
		t.Fatalf("Restore external = %v", err)
	}
	if _, err := NewWithOptions(external, Options[testState, testEvent]{Limits: limits()}); !errors.Is(err, ErrJournalRequired) {
		t.Fatalf("NewWithOptions external without journal = %v", err)
	}
	if _, err := NewWithOptions(external, Options[testState, testEvent]{Limits: limits(), Unjournaled: true}); err != nil {
		t.Fatalf("NewWithOptions unjournaled = %v", err)
	}
	log := &journalLog{}
	if _, err := NewWithOptions(external, Options[testState, testEvent]{Limits: limits(), Journal: log.journal()}); err != nil {
		t.Fatalf("NewWithOptions with journal = %v", err)
	}
	if _, err := New(logicalMachine, limits()); err != nil {
		t.Fatalf("New logical = %v", err)
	}
	// RequireJournal still forces a Journal even for logical machines.
	if _, err := NewWithOptions(logicalMachine, Options[testState, testEvent]{
		Limits: limits(), RequireJournal: true,
	}); !errors.Is(err, ErrJournalRequired) {
		t.Fatalf("RequireJournal logical = %v", err)
	}
}

func TestRestoreValidatesSnapshotSchemaVersion(t *testing.T) {
	machine := MustCompile(validDefinition())
	for _, version := range []uint32{0, SnapshotVersion + 1} {
		snapshot := Snapshot[testState, testEvent]{
			Version: version, DefinitionID: machine.ID(), ExecutionID: "execution", IncarnationID: "stored-incarnation", State: testIdle,
		}
		if _, err := restoreUnjournaled(machine, snapshot, limits()); !errors.Is(err, ErrSnapshotVersion) {
			t.Fatalf("Restore version %d = %v", version, err)
		}
	}
}

func TestRecordsRingBoundsHistoryAndCountsDrops(t *testing.T) {
	machine := MustCompile(validDefinition())
	supervisor, err := NewWithOptions(machine, Options[testState, testEvent]{
		Limits:      Limits{OperationTimeout: time.Second, VerificationTimeout: time.Second, MaxRecords: 3},
		Unjournaled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := supervisor.Start(ctx, &testData{}); err != nil {
		t.Fatal(err)
	}
	// Every refused Issue is one Record; overflow the ring deterministically.
	for range 6 {
		_, _ = supervisor.Issue(ctx, testStop, &testData{})
	}
	records := supervisor.Records()
	if len(records) != 3 {
		t.Fatalf("records length = %d, want 3", len(records))
	}
	for index := 1; index < len(records); index++ {
		if records[index-1].Seq >= records[index].Seq {
			t.Fatalf("record order broken: %+v", records)
		}
	}
	status := supervisor.Status()
	if status.RecordsDropped != 4 { // 7 outcomes recorded, 3 retained
		t.Fatalf("RecordsDropped = %d, want 4; records %+v", status.RecordsDropped, records)
	}
	if records[len(records)-1].Seq != status.Snapshot.Records {
		t.Fatalf("newest record %d != high-water %d", records[len(records)-1].Seq, status.Snapshot.Records)
	}
	if err := validateLimits(Limits{OperationTimeout: 1, VerificationTimeout: 1, MaxRecords: -1}); !errors.Is(err, ErrInvalidLimits) {
		t.Fatalf("negative MaxRecords = %v", err)
	}
}

func TestJournalReceivesPrepareAndClosureInDecisionOrder(t *testing.T) {
	log := &journalLog{}
	machine := MustCompile(validDefinition())
	supervisor, err := NewWithOptions(machine, Options[testState, testEvent]{
		Limits: limits(), Journal: log.journal(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := supervisor.Start(ctx, &testData{}); err != nil {
		t.Fatal(err)
	}
	issued, err := supervisor.Issue(ctx, testStart, &testData{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := supervisor.Verify(ctx, issued.AttemptKey, &testData{verified: true}); err != nil {
		t.Fatal(err)
	}
	writes := log.all()
	if len(writes) != 3 {
		t.Fatalf("journal writes = %+v", writes)
	}
	prepare, closure := writes[1], writes[2]
	if !prepare.InDoubt || prepare.Pending == nil || prepare.Revision != 0 {
		t.Fatalf("prepare = %+v", prepare)
	}
	if closure.InDoubt || closure.Faulted || closure.State != testRunning || closure.Revision != 1 {
		t.Fatalf("closure = %+v", closure)
	}

	// A Trip writes a faulted closure; a successful Recover writes a clean one.
	_ = supervisor.Trip(errTrip)
	if _, err := supervisor.Recover(ctx, &testData{}); err != nil {
		t.Fatal(err)
	}
	writes = log.all()
	if len(writes) != 5 {
		t.Fatalf("journal writes after trip/recover = %+v", writes)
	}
	if !writes[3].Faulted || writes[3].FaultCause == "" {
		t.Fatalf("trip closure = %+v", writes[3])
	}
	if writes[4].Faulted || writes[4].State != testRunning {
		t.Fatalf("recovery closure = %+v", writes[4])
	}
}

func TestJournalClosureFailureIsVisibleWithoutRewritingOutcome(t *testing.T) {
	log := &journalLog{}
	log.fail = func(snapshot Snapshot[testState, testEvent]) error {
		if !snapshot.InDoubt {
			return errors.New("closure store offline")
		}
		return nil
	}
	machine := MustCompile(validDefinition())
	supervisor, err := NewWithOptions(machine, Options[testState, testEvent]{
		Limits: limits(), Journal: log.journal(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, _ = supervisor.Start(ctx, &testData{})
	issued, err := supervisor.Issue(ctx, testStart, &testData{})
	if err != nil {
		t.Fatal(err)
	}
	committed, err := supervisor.Verify(ctx, issued.AttemptKey, &testData{verified: true})
	if err != nil || !committed.Committed {
		t.Fatalf("closure failure changed the commit outcome: %+v, %v", committed, err)
	}
	status := supervisor.Status()
	if !strings.Contains(status.JournalError, "closure store offline") {
		t.Fatalf("JournalError = %q", status.JournalError)
	}
	found := false
	for _, record := range supervisor.Records() {
		if record.Kind == RecordJournalError {
			found = true
			if !strings.Contains(record.CauseText, "closure store offline") {
				t.Fatalf("journal error record lacks cause: %+v", record)
			}
		}
	}
	if !found {
		t.Fatalf("no RecordJournalError in %+v", supervisor.Records())
	}

	// Once the store recovers, the next durable outcome clears the error.
	log.mu.Lock()
	log.fail = nil
	log.mu.Unlock()
	_ = supervisor.Trip(errTrip)
	if status := supervisor.Status(); status.JournalError != "" {
		t.Fatalf("JournalError not cleared: %q", status.JournalError)
	}
}

func TestAdjudicateAdoptCommitsInDoubtDestination(t *testing.T) {
	log := &journalLog{}
	machine := MustCompile(validDefinition())
	supervisor, err := NewWithOptions(machine, Options[testState, testEvent]{
		Limits: limits(), Journal: log.journal(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, _ = supervisor.Start(ctx, &testData{})
	if _, err := supervisor.Issue(ctx, testStart, &testData{}); err != nil {
		t.Fatal(err)
	}
	_ = supervisor.Trip(errTrip)

	adopted, err := supervisor.Adjudicate(ctx, Decision[testState]{
		Outcome: AdjudicateAdopt, Evidence: "controller acknowledged completion; encoder at target",
	}, &testData{})
	if err != nil || !adopted.Committed || adopted.To != testRunning || adopted.Revision != 1 ||
		adopted.Phase != PhaseAdjudicate || adopted.Evidence == "" {
		t.Fatalf("Adjudicate adopt = %+v, %v", adopted, err)
	}
	if snapshot := supervisor.Snapshot(); snapshot.State != testRunning || snapshot.Faulted || snapshot.InDoubt {
		t.Fatalf("Snapshot after adopt = %+v", snapshot)
	}

	records := supervisor.Records()
	last := records[len(records)-1]
	if last.Kind != RecordAdjudicate || last.Evidence != "controller acknowledged completion; encoder at target" ||
		!last.Committed || last.Change.To != testRunning {
		t.Fatalf("adjudication record = %+v", last)
	}
	closure := log.all()[len(log.all())-1]
	if closure.State != testRunning || closure.Faulted || closure.Revision != 1 {
		t.Fatalf("adjudication closure = %+v", closure)
	}
}

func TestAdjudicateRejectsInapplicableDecisions(t *testing.T) {
	supervisor, _ := newUnjournaled(MustCompile(validDefinition()), limits())
	ctx := context.Background()
	_, _ = supervisor.Start(ctx, &testData{})

	if _, err := supervisor.Adjudicate(ctx, Decision[testState]{Outcome: AdjudicateRetain}, &testData{}); !errors.Is(err, ErrNotFaulted) {
		t.Fatalf("adjudicate while ready = %v", err)
	}

	// A fault with no in-doubt external Change cannot be adopted.
	_ = supervisor.Trip(errTrip)
	if _, err := supervisor.Adjudicate(ctx, Decision[testState]{Outcome: AdjudicateAdopt}, &testData{}); !errors.Is(err, ErrInvalidDecision) {
		t.Fatalf("adopt without in-doubt change = %v", err)
	}
	if _, err := supervisor.Adjudicate(ctx, Decision[testState]{
		Outcome: AdjudicateOverride, State: testOther,
	}, &testData{}); !errors.Is(err, ErrInvalidDecision) {
		t.Fatalf("override to undeclared state = %v", err)
	}
	if _, err := supervisor.Adjudicate(ctx, Decision[testState]{}, &testData{}); !errors.Is(err, ErrInvalidDecision) {
		t.Fatalf("zero decision = %v", err)
	}
	if status := supervisor.Status(); status.Mode != ModeFaulted {
		t.Fatalf("refused decisions cleared the fault: %+v", status)
	}

	// Overriding to a declared state commits it and advances Revision.
	overridden, err := supervisor.Adjudicate(ctx, Decision[testState]{
		Outcome: AdjudicateOverride, State: testRunning, Evidence: "minimum-risk condition established",
	}, &testData{})
	if err != nil || !overridden.Committed || overridden.Revision != 1 {
		t.Fatalf("override = %+v, %v", overridden, err)
	}
	if snapshot := supervisor.Snapshot(); snapshot.State != testRunning {
		t.Fatalf("Snapshot after override = %+v", snapshot)
	}
}

func TestAdjudicateReconcilersReceiveProposedState(t *testing.T) {
	var proposed []testState
	definition := validDefinition()
	definition.Reconcile = []Reconciler[testState, testEvent, *testData]{
		func(_ context.Context, snapshot Snapshot[testState, testEvent], _ *testData) error {
			proposed = append(proposed, snapshot.State)
			return nil
		},
	}
	supervisor, _ := newUnjournaled(MustCompile(definition), limits())
	ctx := context.Background()
	_, _ = supervisor.Start(ctx, &testData{})
	if _, err := supervisor.Issue(ctx, testStart, &testData{}); err != nil {
		t.Fatal(err)
	}
	_ = supervisor.Trip(errTrip)
	if _, err := supervisor.Adjudicate(ctx, Decision[testState]{Outcome: AdjudicateAdopt}, &testData{}); err != nil {
		t.Fatal(err)
	}
	if len(proposed) != 2 || proposed[0] != testIdle || proposed[1] != testRunning {
		t.Fatalf("reconcilers saw %v, want [idle running]", proposed)
	}
}

func TestRestoreContinuesRecordIdentityAcrossIncarnations(t *testing.T) {
	log := &journalLog{}
	machine := MustCompile(validDefinition())
	options := Options[testState, testEvent]{Limits: limits(), Journal: log.journal()}
	first, err := NewWithOptions(machine, options)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, _ = first.Start(ctx, &testData{})
	if _, err := first.Issue(ctx, testStart, &testData{}); err != nil {
		t.Fatal(err)
	}
	writes := log.all()
	durable := writes[len(writes)-1] // the in-doubt prepare snapshot
	if !durable.InDoubt || durable.Records == 0 {
		t.Fatalf("prepare snapshot = %+v", durable)
	}

	restored, err := RestoreWithOptions(machine, durable, options)
	if err != nil {
		t.Fatal(err)
	}
	if status := restored.Status(); status.Mode != ModeFaulted || status.Restarts != 1 {
		t.Fatalf("restored status = %+v", status)
	}
	if _, err := restored.Recover(ctx, &testData{}); err != nil {
		t.Fatal(err)
	}
	for _, record := range restored.Records() {
		if record.Restarts != 1 {
			t.Fatalf("record incarnation = %+v", record)
		}
		if record.Seq <= durable.Records {
			t.Fatalf("record seq %d reuses pre-crash identity below %d", record.Seq, durable.Records)
		}
	}
}

func TestSecondaryCauseIsRecordedOnlyForAbandonedCallbacks(t *testing.T) {
	// A promptly failing Issue is the primary cause: exactly one RecordIssue
	// and no RecordSecondaryCause may appear, however the scheduler
	// interleaves the containment goroutine's cleanup.
	prompt := validDefinition()
	prompt.Transitions[0].Issue = func(context.Context, Change[testState, testEvent], *testData) error {
		return errIssue
	}
	supervisor, _ := newUnjournaled(MustCompile(prompt), limits())
	ctx := context.Background()
	_, _ = supervisor.Start(ctx, &testData{})
	if _, err := supervisor.Issue(ctx, testStart, &testData{}); err == nil {
		t.Fatal("failing issue succeeded")
	}
	eventually(t, time.Second, func() bool { return !supervisor.Status().OperationRunning })
	for _, record := range supervisor.Records() {
		if record.Kind == RecordSecondaryCause {
			t.Fatalf("delivered outcome recorded as secondary cause: %+v", record)
		}
	}

	// A callback that outlives its budget and then fails is a genuine
	// secondary cause and must be recorded.
	block := make(chan struct{})
	late := validDefinition()
	late.Transitions[0].Issue = func(context.Context, Change[testState, testEvent], *testData) error {
		<-block
		return errIssue
	}
	abandoned, err := NewWithOptions(MustCompile(late), Options[testState, testEvent]{
		Limits:      Limits{OperationTimeout: 20 * time.Millisecond, VerificationTimeout: time.Second},
		Unjournaled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = abandoned.Start(ctx, &testData{})
	if _, err := abandoned.Issue(ctx, testStart, &testData{}); !errors.Is(err, ErrOperationTimeout) {
		t.Fatalf("Issue = %v", err)
	}
	close(block)
	eventually(t, time.Second, func() bool {
		for _, record := range abandoned.Records() {
			if record.Kind == RecordSecondaryCause {
				return true
			}
		}
		return false
	})
}

func TestNewStringersCoverAdjudicationValues(t *testing.T) {
	if PhaseAdjudicate.String() != "adjudicate" {
		t.Fatalf("PhaseAdjudicate = %q", PhaseAdjudicate.String())
	}
	if RecordAdjudicate.String() != "adjudicate" || RecordJournalError.String() != "journal error" {
		t.Fatalf("record kinds = %q, %q", RecordAdjudicate.String(), RecordJournalError.String())
	}
	for _, adjudication := range []Adjudication{AdjudicateRetain, AdjudicateAdopt, AdjudicateOverride} {
		if adjudication.String() == "" {
			t.Fatalf("empty Adjudication string for %d", adjudication)
		}
	}
	if got := Adjudication(0).String(); got != "Adjudication(0)" {
		t.Fatalf("zero Adjudication = %q", got)
	}
}
