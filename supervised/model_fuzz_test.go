package supervised

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

// The fuzz targets in this file drive the Supervisor against independent
// reference models. Every byte of fuzz input is decoded defensively: any
// []byte must produce either a checked execution or a documented refusal,
// never a panic, deadlock, or invariant violation.

// fuzzIO is the application data for fuzzed executions. Callbacks read the
// failure programmed for the current step.
type fuzzIO struct {
	issueErr     error
	guardErr     error
	verifyErr    error
	invariantErr error
	reconcileErr error
}

var (
	errFuzzIssue     = errors.New("fuzz: issue refused")
	errFuzzGuard     = errors.New("fuzz: guard refused")
	errFuzzJournal   = errors.New("fuzz: journal write rejected")
	errFuzzVerify    = errors.New("fuzz: verification failed")
	errFuzzInvariant = errors.New("fuzz: invariant violated")
	errFuzzReconcile = errors.New("fuzz: reconciliation failed")
)

// fuzzDefinition is a two-state cyclic machine: start is an external
// Issue/Verify transition, stop is purely logical, so one input can exercise
// both durability paths.
func fuzzDefinition() Definition[testState, testEvent, *fuzzIO] {
	return Definition[testState, testEvent, *fuzzIO]{
		ID:      "fuzz-machine/v1",
		Initial: testIdle,
		States: []State[testState, testEvent]{
			{Name: testIdle, Refuse: []testEvent{testStop}},
			{Name: testRunning, Refuse: []testEvent{testStart}},
		},
		Events: []testEvent{testStart, testStop},
		Transitions: []Transition[testState, testEvent, *fuzzIO]{
			{
				ID: "start", From: testIdle, Event: testStart, To: testRunning,
				Issue: func(_ context.Context, _ Change[testState, testEvent], io *fuzzIO) error {
					return io.issueErr
				},
				Verify: func(_ context.Context, _ Change[testState, testEvent], io *fuzzIO) error {
					return io.verifyErr
				},
			},
			{ID: "stop", From: testRunning, Event: testStop, To: testIdle},
		},
		Preconditions: []Precondition[testState, testEvent, *fuzzIO]{
			func(context.Context, Attempt[testState, testEvent], *fuzzIO) error { return nil },
		},
		Invariants: []Check[testState, testEvent, *fuzzIO]{
			func(_ context.Context, _ Change[testState, testEvent], io *fuzzIO) error {
				return io.invariantErr
			},
		},
		Postconditions: []Check[testState, testEvent, *fuzzIO]{
			func(context.Context, Change[testState, testEvent], *fuzzIO) error { return nil },
		},
		Reconcile: []Reconciler[testState, testEvent, *fuzzIO]{
			func(_ context.Context, _ Snapshot[testState, testEvent], io *fuzzIO) error {
				return io.reconcileErr
			},
		},
	}
}

func fuzzSupervisor(
	t testing.TB, maxRecords int, journal Journal[testState, testEvent],
) *Supervisor[testState, testEvent, *fuzzIO] {
	t.Helper()
	supervisor, err := NewWithOptions(MustCompile(fuzzDefinition()), Options[testState, testEvent]{
		Limits:      Limits{OperationTimeout: time.Second, VerificationTimeout: time.Minute, MaxRecords: maxRecords},
		Journal:     journal,
		Unjournaled: journal == nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	return supervisor
}

// ioForStep programs at most one failure per step from one behavior byte.
func ioForStep(behavior byte) *fuzzIO {
	io := &fuzzIO{}
	switch behavior % 8 {
	case 1:
		io.issueErr = errFuzzIssue
	case 2:
		io.verifyErr = errFuzzVerify
	case 3:
		io.invariantErr = errFuzzInvariant
	case 4:
		io.reconcileErr = errFuzzReconcile
	}
	return io
}

// checkStructure asserts the invariants that must hold after every
// synchronous operation, whatever the command history was.
func checkStructure(
	t *testing.T, supervisor *Supervisor[testState, testEvent, *fuzzIO], maxRecords int, priorSeq uint64,
) uint64 {
	t.Helper()
	status := supervisor.Status()
	if (status.Mode == ModeAwaitingVerification) != (status.Pending != nil) {
		t.Fatalf("mode/pending disagree: %+v", status)
	}
	if (status.Mode == ModeFaulted) != (status.Fault != nil) {
		t.Fatalf("mode/fault disagree: %+v", status)
	}
	snapshot := supervisor.Snapshot()
	if snapshot.Version != SnapshotVersion {
		t.Fatalf("snapshot version = %d", snapshot.Version)
	}
	if snapshot.ExecutionID == "" || snapshot.IncarnationID == "" {
		t.Fatalf("snapshot has an empty execution identity: %+v", snapshot)
	}
	if snapshot.InDoubt != (snapshot.Pending != nil) {
		t.Fatalf("snapshot in-doubt/pending disagree: %+v", snapshot)
	}
	records := supervisor.Records()
	if len(records) > maxRecords {
		t.Fatalf("records length %d exceeds bound %d", len(records), maxRecords)
	}
	lastSeq := priorSeq
	for index, record := range records {
		if index > 0 && records[index-1].Seq >= record.Seq {
			t.Fatalf("record seq not strictly increasing: %d then %d", records[index-1].Seq, record.Seq)
		}
		if record.Seq == 0 || record.ExecutionID != snapshot.ExecutionID ||
			record.IncarnationID != snapshot.IncarnationID || record.Restarts != snapshot.Restarts {
			t.Fatalf("record identity does not match its execution: %+v vs %+v", record, snapshot)
		}
		if record.Seq > lastSeq {
			lastSeq = record.Seq
		}
	}
	if lastSeq > snapshot.Records {
		t.Fatalf("record seq %d beyond snapshot high-water %d", lastSeq, snapshot.Records)
	}
	if lastSeq < priorSeq {
		t.Fatalf("record history moved backwards: %d then %d", priorSeq, lastSeq)
	}
	return lastSeq
}

// FuzzSupervisorModel exercises arbitrary synchronous lifecycle command
// sequences with per-step programmed callback failures and checks the
// externally visible mode, pending, fault, revision, and record model after
// every operation against an exact reference state machine.
func FuzzSupervisorModel(f *testing.F) {
	f.Add([]byte{0, 8, 1, 8})
	f.Add([]byte{16, 24, 0, 8, 1, 8})
	f.Add([]byte{0, 9, 2, 3, 8, 4, 8})
	f.Add([]byte{0, 8, 32, 5, 8, 0, 10, 6, 8})
	f.Add([]byte{7, 3, 0, 8, 40, 48, 56, 1})
	f.Fuzz(func(t *testing.T, program []byte) {
		if len(program) > 64 {
			program = program[:64]
		}
		maxRecords := 8
		supervisor := fuzzSupervisor(t, maxRecords, nil)
		ctx := context.Background()

		type modelState struct {
			mode     Mode
			state    testState
			revision uint64
		}
		model := modelState{mode: ModeStopped, state: testIdle}
		var pendingKey AttemptID
		var lastSeq uint64

		for index := 0; index < len(program); index++ {
			command := program[index] % 8
			behavior := program[index] / 8
			io := ioForStep(behavior)

			switch command {
			case 0: // Start
				result, err := supervisor.Start(ctx, io)
				switch model.mode {
				case ModeStopped:
					if io.reconcileErr != nil {
						if err == nil || !result.Faulted {
							t.Fatalf("start with failing reconcile = %+v, %v", result, err)
						}
						model.mode = ModeFaulted
					} else {
						if err != nil {
							t.Fatalf("start = %+v, %v", result, err)
						}
						model.mode = ModeReady
					}
				case ModeReady, ModeAwaitingVerification:
					if !errors.Is(err, ErrAlreadyStarted) {
						t.Fatalf("start in %v = %v", model.mode, err)
					}
				case ModeFaulted:
					if err == nil {
						t.Fatalf("start while faulted succeeded: %+v", result)
					}
				}
			case 1: // Issue start
				result, err := supervisor.Issue(ctx, testStart, io)
				switch {
				case model.mode == ModeStopped:
					if !errors.Is(err, ErrNotStarted) {
						t.Fatalf("issue while stopped = %v", err)
					}
				case model.mode == ModeAwaitingVerification:
					if !errors.Is(err, ErrAwaitingVerification) {
						t.Fatalf("issue while awaiting = %v", err)
					}
				case model.mode == ModeFaulted:
					if err == nil {
						t.Fatalf("issue while faulted = %+v", result)
					}
				case model.state == testRunning:
					if !errors.Is(err, ErrNotPermitted) || result.Faulted {
						t.Fatalf("refused issue = %+v, %v", result, err)
					}
				case io.invariantErr != nil:
					if !errors.Is(err, errFuzzInvariant) || !result.Faulted {
						t.Fatalf("invariant issue = %+v, %v", result, err)
					}
					model.mode = ModeFaulted
				case io.issueErr != nil:
					if !errors.Is(err, errFuzzIssue) || !result.Faulted || !result.Uncertain {
						t.Fatalf("failing issue = %+v, %v", result, err)
					}
					model.mode = ModeFaulted
				default:
					if err != nil || !result.IssueCompleted || result.Committed {
						t.Fatalf("issue = %+v, %v", result, err)
					}
					model.mode = ModeAwaitingVerification
					pendingKey = result.AttemptKey
				}
			case 2: // Issue stop (logical)
				result, err := supervisor.Issue(ctx, testStop, io)
				switch {
				case model.mode == ModeStopped, model.mode == ModeFaulted, model.mode == ModeAwaitingVerification:
					if err == nil {
						t.Fatalf("stop in %v = %+v", model.mode, result)
					}
				case model.state == testIdle:
					if !errors.Is(err, ErrNotPermitted) || result.Faulted {
						t.Fatalf("refused stop = %+v, %v", result, err)
					}
				case io.invariantErr != nil:
					if !errors.Is(err, errFuzzInvariant) || !result.Faulted {
						t.Fatalf("invariant stop = %+v, %v", result, err)
					}
					model.mode = ModeFaulted
				default:
					if err != nil || !result.Committed {
						t.Fatalf("stop = %+v, %v", result, err)
					}
					model.state = testIdle
					model.revision++
				}
			case 3: // Verify with the pending identifier
				result, err := supervisor.Verify(ctx, pendingKey, io)
				switch {
				case model.mode != ModeAwaitingVerification:
					if err == nil {
						t.Fatalf("verify in %v = %+v", model.mode, result)
					}
				case io.verifyErr != nil:
					if !errors.Is(err, errFuzzVerify) || !result.Faulted || !result.Uncertain {
						t.Fatalf("failing verify = %+v, %v", result, err)
					}
					model.mode = ModeFaulted
				case io.invariantErr != nil:
					if !errors.Is(err, errFuzzInvariant) || !result.Faulted || !result.Uncertain {
						t.Fatalf("post-verify invariant = %+v, %v", result, err)
					}
					model.mode = ModeFaulted
				default:
					if err != nil || !result.Committed || !result.Verified {
						t.Fatalf("verify = %+v, %v", result, err)
					}
					model.mode = ModeReady
					model.state = testRunning
					model.revision++
				}
			case 4: // Verify with a stale identifier
				result, err := supervisor.Verify(ctx, AttemptID{ExecutionID: supervisor.Snapshot().ExecutionID, Sequence: math.MaxUint32}, io)
				if model.mode == ModeAwaitingVerification {
					if !errors.Is(err, ErrStaleAttempt) || result.Faulted {
						t.Fatalf("stale verify = %+v, %v", result, err)
					}
				} else if err == nil {
					t.Fatalf("stale verify in %v = %+v", model.mode, result)
				}
			case 5: // Trip
				fault := supervisor.Trip(errTrip)
				if fault.CauseText == "" {
					t.Fatalf("trip fault = %+v", fault)
				}
				model.mode = ModeFaulted
			case 6: // Recover
				result, err := supervisor.Recover(ctx, io)
				switch {
				case model.mode != ModeFaulted:
					if !errors.Is(err, ErrNotFaulted) {
						t.Fatalf("recover in %v = %v", model.mode, err)
					}
				case errors.Is(err, ErrCallbackRunning):
					// A revoked operation's contained callback can still be
					// unwinding for an instant after its fault latched;
					// recovery is refused until the whole operation ends and
					// the fault stays latched. This is LIB-SUP-001 behavior.
				case io.reconcileErr != nil:
					if !errors.Is(err, errFuzzReconcile) || !result.Faulted {
						t.Fatalf("failing recover = %+v, %v", result, err)
					}
				default:
					if err != nil || result.Faulted {
						t.Fatalf("recover = %+v, %v", result, err)
					}
					model.mode = ModeReady
				}
			case 7: // Adjudicate, outcome selected by behavior
				decision := Decision[testState]{Outcome: Adjudication(behavior%3) + 1, Evidence: "fuzz"}
				undeclared := behavior%4 == 3
				if decision.Outcome == AdjudicateOverride {
					decision.State = testIdle
				}
				if undeclared {
					decision.Outcome = AdjudicateOverride
					decision.State = testOther // undeclared
				}
				fault := supervisor.Status().Fault
				result, err := supervisor.Adjudicate(ctx, decision, io)
				switch {
				case model.mode != ModeFaulted:
					if !errors.Is(err, ErrNotFaulted) {
						t.Fatalf("adjudicate in %v = %v", model.mode, err)
					}
				case errors.Is(err, ErrCallbackRunning):
					// Transient refusal while the revoked callback unwinds;
					// see the Recover case above.
				case undeclared:
					if !errors.Is(err, ErrInvalidDecision) {
						t.Fatalf("undeclared override = %v", err)
					}
				case decision.Outcome == AdjudicateAdopt &&
					(fault == nil || fault.TransitionID == "" || fault.IssuedAt.IsZero()):
					if !errors.Is(err, ErrInvalidDecision) {
						t.Fatalf("adopt without in-doubt change = %v", err)
					}
				case io.reconcileErr != nil:
					if err == nil {
						t.Fatalf("adjudicate with failing reconcile = %+v", result)
					}
				default:
					if err != nil || result.Faulted {
						t.Fatalf("adjudicate = %+v, %v", result, err)
					}
					model.mode = ModeReady
					target := model.state
					switch decision.Outcome {
					case AdjudicateAdopt:
						target = fault.To
					case AdjudicateOverride:
						target = decision.State
					}
					if decision.Outcome != AdjudicateRetain {
						model.state = target
						model.revision++
					}
				}
			}

			lastSeq = checkStructure(t, supervisor, maxRecords, lastSeq)
			snapshot := supervisor.Snapshot()
			status := supervisor.Status()
			if status.Mode != model.mode {
				t.Fatalf("step %d: mode = %v, model %v (program %v)", index, status.Mode, model.mode, program)
			}
			if snapshot.State != model.state {
				t.Fatalf("step %d: state = %v, model %v (program %v)", index, snapshot.State, model.state, program)
			}
			if snapshot.Revision != model.revision {
				t.Fatalf("step %d: revision = %d, model %d (program %v)", index, snapshot.Revision, model.revision, program)
			}
		}
	})
}

// lastSnapshotJournal models durable application storage outside a Supervisor
// lifetime. A rejected write leaves the old image available for repeated restores.
type lastSnapshotJournal struct {
	mu     sync.Mutex
	wrote  bool
	reject bool
	last   Snapshot[testState, testEvent]
}

func (j *lastSnapshotJournal) journal() Journal[testState, testEvent] {
	return func(_ context.Context, snapshot Snapshot[testState, testEvent]) error {
		j.mu.Lock()
		defer j.mu.Unlock()
		if j.reject {
			return errFuzzJournal
		}
		j.wrote = true
		j.last = snapshot
		return nil
	}
}

func (j *lastSnapshotJournal) rejectWrites(reject bool) {
	j.mu.Lock()
	j.reject = reject
	j.mu.Unlock()
}

func (j *lastSnapshotJournal) snapshot() (Snapshot[testState, testEvent], bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.last, j.wrote
}

// restartAudit models independent application and controller histories. It
// survives every restore and never seeds its identity sets from the Journal or
// the Supervisor's bounded Records view. A rejected durable closure cannot erase
// identities that an external callback has already observed.
type restartAudit struct {
	mu       sync.Mutex
	records  map[restartRecordID]bool
	attempts map[AttemptID]bool
	issued   map[AttemptID]bool
	problem  error
}

type restartRecordID struct {
	execution   string
	incarnation string
	seq         uint64
}

func (a *restartAudit) record(_ context.Context, record Record[testState, testEvent]) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.records == nil {
		a.records = make(map[restartRecordID]bool)
	}
	id := restartRecordID{record.ExecutionID, record.IncarnationID, record.Seq}
	if id.execution == "" || id.incarnation == "" || id.seq == 0 || a.records[id] {
		a.problem = fmt.Errorf("invalid or reused external Record identity: %+v", id)
	}
	a.records[id] = true
	return nil
}

func (a *restartAudit) attempt(id AttemptID, issued bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.attempts == nil {
		a.attempts = make(map[AttemptID]bool)
		a.issued = make(map[AttemptID]bool)
	}
	seen := a.attempts
	if issued {
		seen = a.issued
		if !a.attempts[id] {
			a.problem = fmt.Errorf("controller saw an unaccepted Attempt: %+v", id)
		}
	}
	if id.ExecutionID == "" || id.IncarnationID == "" || id.Sequence == 0 || seen[id] {
		a.problem = fmt.Errorf("invalid or reused external Attempt identity (issued=%v): %+v", issued, id)
	}
	seen[id] = true
}

func (a *restartAudit) check(t *testing.T) {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.problem != nil {
		t.Fatal(a.problem)
	}
}

// FuzzSupervisorRestartModel keeps independent Recorder, accepted Attempt,
// controller Issue, and Journal histories across arbitrary power-loss points.
// Clean startup can fail reconciliation or its Journal closure; guard-refused
// Attempts can also outlive the latest durable image. Every new incarnation
// must remain distinct even when the same stale image is restored repeatedly.
func FuzzSupervisorRestartModel(f *testing.F) {
	f.Add([]byte{0, 0, 2, 0, 255, 0})
	f.Add([]byte{0, 0, 255, 1, 0, 0, 2, 0, 255, 2})
	f.Add([]byte{0, 8, 255, 0, 255, 1, 255, 2, 255, 3})
	f.Add([]byte{0, 0, 2, 16, 255, 3, 0, 0, 255, 0})
	f.Add([]byte{255, 4, 255, 4, 255, 0})              // clean startup cannot save its new incarnation
	f.Add([]byte{255, 12, 255, 12, 255, 0})            // failed startup cannot save its fault
	f.Add([]byte{0, 16, 255, 4, 0, 16, 255, 4, 0, 16}) // guard refusals before stale restores
	f.Fuzz(func(t *testing.T, program []byte) {
		if len(program) > 48 {
			program = program[:48]
		}
		audit := &restartAudit{}
		definition := fuzzDefinition()
		definition.Preconditions[0] = func(_ context.Context, attempt Attempt[testState, testEvent], _ *fuzzIO) error {
			audit.attempt(attempt.Identifier(), false)
			return nil
		}
		definition.Transitions[0].Guard = func(_ context.Context, _ Change[testState, testEvent], io *fuzzIO) error {
			return io.guardErr
		}
		definition.Transitions[0].Issue = func(_ context.Context, change Change[testState, testEvent], io *fuzzIO) error {
			audit.attempt(change.Identifier(), true)
			return io.issueErr
		}
		machine := MustCompile(definition)
		store := &lastSnapshotJournal{}
		options := func() Options[testState, testEvent] {
			return Options[testState, testEvent]{
				Limits:  Limits{OperationTimeout: time.Second, VerificationTimeout: time.Minute, MaxRecords: 8},
				Journal: store.journal(), Recorder: audit.record,
				// A dead process cannot later dispatch its verification timer.
				Clock: newFakeClock(),
			}
		}
		supervisor, err := NewWithOptions(machine, options())
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		// The application checkpoints the lineage before any startup callback.
		if err := store.journal()(ctx, supervisor.Snapshot()); err != nil {
			t.Fatal(err)
		}
		if _, err := supervisor.Start(ctx, &fuzzIO{}); err != nil {
			t.Fatal(err)
		}
		executionID := supervisor.Snapshot().ExecutionID
		incarnations := map[string]bool{supervisor.Snapshot().IncarnationID: true}
		audit.check(t)

		for index := 0; index < len(program)-1; index += 2 {
			command, behavior := program[index], program[index+1]
			store.rejectWrites(behavior&4 != 0)
			if command != 255 {
				io := ioForStep(behavior)
				if behavior&16 != 0 {
					io.guardErr = errFuzzGuard
				}
				switch command % 6 {
				case 0:
					_, _ = supervisor.Issue(ctx, testStart, io)
				case 1:
					_, _ = supervisor.Issue(ctx, testStop, io)
				case 2:
					if pending := supervisor.Status().Pending; pending != nil {
						_, _ = supervisor.Verify(ctx, pending.Identifier(), io)
					}
				case 3:
					_, _ = supervisor.Recover(ctx, io)
				case 4:
					_, _ = supervisor.Start(ctx, io)
				case 5:
					_ = supervisor.Trip(errTrip)
				}
				audit.check(t)
				checkStructure(t, supervisor, 8, 0)
				continue
			}

			// Only the Journal image survives locally; all external audit sets do.
			durable, wrote := store.snapshot()
			if !wrote {
				t.Fatal("birth checkpoint was lost")
			}
			restored, err := RestoreWithOptions(machine, durable, options())
			if err != nil {
				t.Fatalf("restore from %+v = %v", durable, err)
			}
			status := restored.Status()
			if status.Restarts != durable.Restarts+1 {
				t.Fatalf("restarts = %d, want %d", status.Restarts, durable.Restarts+1)
			}
			snapshot := restored.Snapshot()
			if snapshot.ExecutionID != executionID || snapshot.IncarnationID == "" || incarnations[snapshot.IncarnationID] {
				t.Fatalf("invalid restored identity: %+v", snapshot)
			}
			incarnations[snapshot.IncarnationID] = true
			if (status.Mode == ModeFaulted) != (durable.InDoubt || durable.Faulted) {
				t.Fatalf("restored mode = %v for snapshot %+v", status.Mode, durable)
			}
			io := &fuzzIO{}
			if behavior&8 != 0 {
				io.reconcileErr = errFuzzReconcile
			}
			if status.Mode == ModeFaulted {
				fault := status.Fault
				decision := Decision[testState]{Outcome: AdjudicateRetain, Evidence: "fuzz restart"}
				if behavior%2 == 1 && fault != nil && fault.TransitionID != "" && !fault.IssuedAt.IsZero() {
					decision.Outcome = AdjudicateAdopt
				}
				result, err := restored.Adjudicate(ctx, decision, io)
				if io.reconcileErr != nil {
					if !errors.Is(err, errFuzzReconcile) || !result.Faulted {
						t.Fatalf("failed reconciliation after restart = %+v, %v", result, err)
					}
				} else {
					if err != nil || result.Evidence != "fuzz restart" {
						t.Fatalf("adjudicate after restart = %+v, %v", result, err)
					}
					if decision.Outcome == AdjudicateAdopt && restored.Snapshot().State != fault.To {
						t.Fatalf("adopt did not commit destination: %+v", restored.Snapshot())
					}
				}
			} else {
				result, err := restored.Start(ctx, io)
				if io.reconcileErr != nil {
					if !errors.Is(err, errFuzzReconcile) || !result.Faulted {
						t.Fatalf("failed clean startup = %+v, %v", result, err)
					}
				} else if err != nil {
					t.Fatalf("start after clean restore = %v", err)
				}
			}
			audit.check(t)
			for _, record := range restored.Records() {
				if record.IncarnationID != snapshot.IncarnationID || record.Restarts != durable.Restarts+1 || record.Seq <= durable.Records {
					t.Fatalf("restored Record does not extend its image: %+v (image %+v)", record, durable)
				}
			}
			if restored.Snapshot().Revision < durable.Revision {
				t.Fatalf("revision regressed after restore: %d < %d", restored.Snapshot().Revision, durable.Revision)
			}
			supervisor = restored
		}
	})
}

// FuzzAdversarialClock drives operations under an injected Clock whose
// advancement and timer dispatch are adversarial: time leaps arbitrarily,
// timers fire late, repeatedly, or not at all. Deadline admission must remain
// exact and the structural model must hold throughout.
func FuzzAdversarialClock(f *testing.F) {
	f.Add([]byte{0, 1, 0, 2, 30, 1})
	f.Add([]byte{0, 1, 61, 2, 2, 1})
	f.Add([]byte{0, 1, 200, 4, 4, 2})
	f.Add([]byte{0, 1, 59, 3, 5, 2, 6})
	f.Fuzz(func(t *testing.T, program []byte) {
		if len(program) > 48 {
			program = program[:48]
		}
		clock := newFakeClock()
		supervisor, err := NewWithOptions(MustCompile(fuzzDefinition()), Options[testState, testEvent]{
			Limits:      Limits{OperationTimeout: time.Hour, VerificationTimeout: time.Minute, MaxRecords: 16},
			Clock:       clock,
			Unjournaled: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		if _, err := supervisor.Start(ctx, &fuzzIO{}); err != nil {
			t.Fatal(err)
		}
		var deadline time.Time

		for _, command := range program {
			switch command % 7 {
			case 0: // Issue the external transition when possible.
				result, err := supervisor.Issue(ctx, testStart, &fuzzIO{})
				if err == nil && result.IssueCompleted {
					deadline = supervisor.Status().VerificationDeadline
					if !deadline.Equal(clock.Now().Add(time.Minute)) {
						t.Fatalf("verification deadline = %v, want %v", deadline, clock.Now().Add(time.Minute))
					}
				}
			case 1: // Verify: admission must depend only on the stored deadline.
				status := supervisor.Status()
				if status.Pending == nil {
					continue
				}
				expired := !clock.Now().Before(deadline)
				result, err := supervisor.Verify(ctx, status.Pending.Identifier(), &fuzzIO{})
				if expired {
					if !errors.Is(err, ErrVerificationTimeout) || !result.Faulted {
						t.Fatalf("expired verify admitted: %+v, %v (now %v deadline %v)", result, err, clock.Now(), deadline)
					}
				} else if err != nil || !result.Committed {
					t.Fatalf("in-window verify = %+v, %v", result, err)
				}
			case 2: // Advance the clock by a byte-scaled amount.
				clock.advance(time.Duration(command) * time.Second)
			case 3: // Dispatch due timers, possibly very late.
				clock.fireDue()
			case 4: // Dispatch twice: repeated notification must be harmless.
				clock.fireDue()
				clock.fireDue()
			case 5:
				_, _ = supervisor.Recover(ctx, &fuzzIO{})
			case 6:
				_, _ = supervisor.Issue(ctx, testStop, &fuzzIO{})
			}
			status := supervisor.Status()
			if (status.Mode == ModeAwaitingVerification) != (status.Pending != nil) {
				t.Fatalf("mode/pending disagree: %+v", status)
			}
			if (status.Mode == ModeFaulted) != (status.Fault != nil) {
				t.Fatalf("mode/fault disagree: %+v", status)
			}
			records := supervisor.Records()
			for index := 1; index < len(records); index++ {
				if records[index-1].Seq >= records[index].Seq {
					t.Fatalf("record seq order broken: %+v", records)
				}
			}
		}
	})
}

// FuzzConcurrentSupervisor schedules byte-assigned operations across several
// goroutines. The assertions are freedom from deadlock and data races (under
// -race), fault/pending coherence at every observation, and strictly
// monotonic, never-duplicated record sequence numbers afterward.
func FuzzConcurrentSupervisor(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 0, 1})
	f.Add([]byte{1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3})
	f.Add([]byte{5, 0, 5, 1, 5, 2, 5, 3})
	f.Fuzz(func(t *testing.T, program []byte) {
		if len(program) > 32 {
			program = program[:32]
		}
		supervisor := fuzzSupervisor(t, 64, nil)
		ctx := context.Background()
		_, _ = supervisor.Start(ctx, &fuzzIO{})

		workers := 3
		var violations sync.Map
		var wg sync.WaitGroup
		for worker := range workers {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				for index := worker; index < len(program); index += workers {
					switch program[index] % 6 {
					case 0:
						_, _ = supervisor.Issue(ctx, testStart, &fuzzIO{})
					case 1:
						if pending := supervisor.Status().Pending; pending != nil {
							_, _ = supervisor.Verify(ctx, pending.Identifier(), &fuzzIO{})
						}
					case 2:
						_ = supervisor.Trip(errTrip)
					case 3:
						_, _ = supervisor.Recover(ctx, &fuzzIO{})
					case 4:
						_, _ = supervisor.Issue(ctx, testStop, &fuzzIO{})
					case 5:
						// Mid-operation observations can legitimately pair
						// ModeExecuting with a still-latched Fault (Recover
						// and Adjudicate clear it only on success), so only
						// the forward implications are invariant.
						status := supervisor.Status()
						if status.Mode == ModeFaulted && status.Fault == nil {
							violations.Store("faulted without fault", status.Mode)
						}
						if status.Mode == ModeAwaitingVerification && status.Pending == nil {
							violations.Store("awaiting without pending", status.Mode)
						}
						_ = supervisor.Records()
						_ = supervisor.Snapshot()
					}
				}
			}(worker)
		}
		wg.Wait()
		violations.Range(func(key, _ any) bool {
			t.Fatalf("concurrent invariant violated: %v", key)
			return false
		})

		records := supervisor.Records()
		seen := make(map[uint64]bool, len(records))
		for index, record := range records {
			if seen[record.Seq] {
				t.Fatalf("duplicate record seq %d", record.Seq)
			}
			seen[record.Seq] = true
			if index > 0 && records[index-1].Seq >= record.Seq {
				t.Fatalf("record seq order broken: %+v", records)
			}
		}
	})
}

// FuzzSnapshotRestore mutates otherwise-valid Snapshots field by field.
// Accepted inputs must survive real public lifecycle operations, including
// counter-exhaustion refusals, rather than only a structural round trip.
func FuzzSnapshotRestore(f *testing.F) {
	f.Add(uint8(0), uint64(0), uint64(0), uint64(0), false, false, "execution")
	f.Add(uint8(1), uint64(3), uint64(2), uint64(9), false, false, "execution")
	f.Add(uint8(2), uint64(1), uint64(1), uint64(1), false, true, "execution")
	f.Add(uint8(7), uint64(5), uint64(5), uint64(5), true, false, "execution")
	f.Add(uint8(0), uint64(5), uint64(5), uint64(5), false, true, "execution")
	f.Add(uint8(0), uint64(math.MaxUint64-1), uint64(5), uint64(5), true, false, "execution")
	f.Add(uint8(0), uint64(5), uint64(math.MaxUint64), uint64(5), false, false, "execution")
	f.Add(uint8(0), uint64(5), uint64(5), uint64(math.MaxUint64-17), true, false, "execution")
	f.Fuzz(func(t *testing.T, mutation uint8, revision, attempt, records uint64, faulted, inDoubt bool, executionID string) {
		machine := MustCompile(fuzzDefinition())
		snapshot := Snapshot[testState, testEvent]{
			Version: SnapshotVersion, DefinitionID: machine.ID(),
			ExecutionID: executionID, IncarnationID: "stored-incarnation",
			State: testIdle, Revision: revision, Attempt: attempt, Records: records,
			Faulted: faulted, RecordedAt: time.Unix(1754000000, 0),
		}
		if inDoubt {
			snapshot.InDoubt = true
			snapshot.Uncertain = true
			snapshot.Pending = &PendingSnapshot[testState, testEvent]{
				Attempt:  AttemptID{ExecutionID: executionID, IncarnationID: snapshot.IncarnationID, Sequence: attempt},
				Revision: revision, From: testIdle, Event: testStart,
				TransitionID: "start", To: testRunning, IssuedAt: snapshot.RecordedAt,
			}
		}
		switch mutation % 10 {
		case 1:
			snapshot.Version = SnapshotVersion + 1
		case 2:
			snapshot.DefinitionID = "someone-else/v9"
		case 3:
			snapshot.State = testOther
		case 4:
			if snapshot.Pending != nil {
				snapshot.Pending.TransitionID = "unknown"
			}
		case 5:
			snapshot.InDoubt = !snapshot.InDoubt
		case 6:
			if snapshot.Pending != nil {
				snapshot.Pending.Attempt.Sequence = attempt + 1
			}
		case 7:
			snapshot.Restarts = math.MaxUint64
		case 8:
			snapshot.IncarnationID = ""
		case 9:
			if snapshot.Pending != nil {
				snapshot.Pending.Attempt.IncarnationID = ""
			}
		}

		audit := &restartAudit{}
		restored, err := RestoreWithOptions(machine, snapshot, Options[testState, testEvent]{
			Limits:      Limits{OperationTimeout: time.Second, VerificationTimeout: time.Second, MaxRecords: 16},
			Unjournaled: true, Recorder: audit.record,
		})
		if err != nil {
			for _, sentinel := range []error{
				ErrSnapshotVersion, ErrDefinitionMismatch, ErrInvalidSnapshot,
				ErrUnknownState, ErrCounterExhausted,
			} {
				if errors.Is(err, sentinel) {
					return
				}
			}
			t.Fatalf("undocumented restore error: %v", err)
		}
		roundTrip := restored.Snapshot()
		if roundTrip.ExecutionID != snapshot.ExecutionID || roundTrip.State != snapshot.State ||
			roundTrip.Revision != snapshot.Revision || roundTrip.Attempt != snapshot.Attempt ||
			roundTrip.Restarts != snapshot.Restarts+1 || roundTrip.Records != snapshot.Records ||
			roundTrip.IncarnationID == "" || roundTrip.IncarnationID == snapshot.IncarnationID {
			t.Fatalf("restore identity mismatch: %+v vs %+v", roundTrip, snapshot)
		}
		defer func() {
			audit.check(t)
			checkStructure(t, restored, 16, snapshot.Records)
			after := restored.Snapshot()
			if after.Revision < snapshot.Revision || after.Attempt < snapshot.Attempt || after.Records < snapshot.Records {
				t.Fatalf("public operation wrapped a restored counter: %+v vs %+v", after, snapshot)
			}
		}()
		ctx := context.Background()
		if snapshot.InDoubt || snapshot.Faulted {
			if _, err := restored.Start(ctx, &fuzzIO{}); err == nil {
				t.Fatal("faulted restoration started without recovery")
			}
			result, err := restored.Recover(ctx, &fuzzIO{})
			if errors.Is(err, ErrCounterExhausted) && result.Faulted && !result.Committed {
				return // the refused Start may consume the last admission headroom
			}
			if err != nil || result.Faulted || result.Committed {
				t.Fatalf("restored recovery = %+v, %v", result, err)
			}
		} else {
			if result, err := restored.Start(ctx, &fuzzIO{}); err != nil {
				t.Fatalf("restored startup = %+v, %v", result, err)
			}
			if _, err := restored.Recover(ctx, &fuzzIO{}); !errors.Is(err, ErrNotFaulted) {
				t.Fatalf("recovery of clean execution = %v", err)
			}
		}
		audit.check(t)
		issued, err := restored.Issue(ctx, testStart, &fuzzIO{})
		if err != nil {
			if !errors.Is(err, ErrCounterExhausted) || issued.IssueCompleted {
				t.Fatalf("restored Issue = %+v, %v", issued, err)
			}
		} else {
			if issued.AttemptKey.ExecutionID != executionID || issued.AttemptKey.IncarnationID != roundTrip.IncarnationID ||
				issued.AttemptKey.Sequence <= snapshot.Attempt || !issued.IssueCompleted {
				t.Fatalf("restored Attempt identity = %+v (snapshot %+v)", issued, snapshot)
			}
			verified, err := restored.Verify(ctx, issued.AttemptKey, &fuzzIO{})
			if err != nil {
				if !errors.Is(err, ErrCounterExhausted) || verified.Committed {
					t.Fatalf("restored Verification = %+v, %v", verified, err)
				}
			} else if !verified.Committed || restored.Snapshot().State != testRunning {
				t.Fatalf("restored Verification did not commit: %+v", verified)
			}
		}
	})
}

// FuzzDefinitionCompile feeds arbitrary strict Definitions to Compile, which
// must never panic. When compilation succeeds, the completeness guarantees
// the compiler promises must actually hold, and MustCompile must agree.
func FuzzDefinitionCompile(f *testing.F) {
	f.Add([]byte{2, 2, 0, 0, 0, 0, 1, 1}, "id")
	f.Add([]byte{1, 1, 0, 0, 0, 0, 0, 0}, "")
	f.Add([]byte{3, 2, 9, 9, 9, 1, 1, 2}, "loop")
	f.Add([]byte{2, 1, 0, 3, 0, 0, 1, 1}, "dup")
	f.Fuzz(func(t *testing.T, shape []byte, id string) {
		if len(shape) < 4 {
			return
		}
		if len(shape) > 40 {
			shape = shape[:40]
		}
		stateCount := int(shape[0]%4) + 1
		eventCount := int(shape[1]%3) + 1
		names := []testState{testIdle, testRunning, testOther, testState("fourth")}
		eventNames := []testEvent{testStart, testStop, testEvent("third")}

		definition := Definition[testState, testEvent, *fuzzIO]{ID: id, Initial: names[int(shape[2])%stateCount]}
		for index := range stateCount {
			state := State[testState, testEvent]{Name: names[index]}
			if shape[3]&(1<<index) != 0 {
				state.Terminal = true
			} else {
				// Refuse everything by default so completeness depends only on
				// the fuzzed transitions overriding it; Compile must reject the
				// overlap when a transition handles a refused event.
				state.Refuse = append([]testEvent(nil), eventNames[:eventCount]...)
			}
			definition.States = append(definition.States, state)
		}
		definition.Events = append([]testEvent(nil), eventNames[:eventCount]...)
		pass := func(context.Context, Change[testState, testEvent], *fuzzIO) error { return nil }
		definition.Preconditions = []Precondition[testState, testEvent, *fuzzIO]{
			func(context.Context, Attempt[testState, testEvent], *fuzzIO) error { return nil },
		}
		definition.Invariants = []Check[testState, testEvent, *fuzzIO]{pass}
		definition.Postconditions = []Check[testState, testEvent, *fuzzIO]{pass}
		definition.Reconcile = []Reconciler[testState, testEvent, *fuzzIO]{
			func(context.Context, Snapshot[testState, testEvent], *fuzzIO) error { return nil },
		}
		for index := 4; index+3 < len(shape); index += 4 {
			transition := Transition[testState, testEvent, *fuzzIO]{
				ID:    string(rune('a' + index)),
				From:  names[int(shape[index])%len(names)],
				Event: eventNames[int(shape[index+1])%len(eventNames)],
				To:    names[int(shape[index+2])%len(names)],
			}
			if shape[index+3]%2 == 1 {
				transition.Issue = func(context.Context, Change[testState, testEvent], *fuzzIO) error { return nil }
				transition.Verify = pass
			}
			definition.Transitions = append(definition.Transitions, transition)
		}

		machine, err := Compile(definition)
		panicked := func() (panicked bool) {
			defer func() { panicked = recover() != nil }()
			MustCompile(definition)
			return false
		}()
		if (err != nil) != panicked {
			t.Fatalf("Compile error %v but MustCompile panic %v", err, panicked)
		}
		if err != nil {
			return
		}
		wantExternal := false
		for _, transition := range definition.Transitions {
			if transition.Issue != nil {
				wantExternal = true
			}
		}
		if machine.External() != wantExternal {
			t.Fatalf("External() = %v, want %v", machine.External(), wantExternal)
		}
		for state := range machine.States() {
			if state.Terminal {
				continue
			}
			for event := range machine.Events() {
				handled := false
				for transition := range machine.Transitions() {
					if transition.From == state.Name && transition.Event == event {
						handled = true
					}
				}
				if !handled && !machine.Refused(state.Name, event) {
					t.Fatalf("compiled machine leaves %v unhandled in %v", event, state.Name)
				}
			}
		}
	})
}

// faultyAdapters injects Recorder and Journal failures — errors and panics —
// while counting every delivery, so the accounting in Status can be checked
// exactly.
type faultyAdapters struct {
	mu               sync.Mutex
	program          []byte
	recorderCalls    int
	recorderFailures int
	journalCalls     int
}

func (a *faultyAdapters) step(calls int) byte {
	if len(a.program) == 0 {
		return 0
	}
	return a.program[calls%len(a.program)]
}

// FuzzRecorderJournalFaultInjection drives the Supervisor while its Recorder
// and Journal fail in programmed ways. Failures must be visible and counted,
// must never rewrite a completed outcome, and a failed preparation must
// refuse the Issue outright.
func FuzzRecorderJournalFaultInjection(f *testing.F) {
	f.Add([]byte{0, 1, 2, 0})
	f.Add([]byte{1, 1, 1, 1})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{2, 0, 1, 0, 2})
	f.Fuzz(func(t *testing.T, program []byte) {
		adapters := &faultyAdapters{program: program}
		fail := func(mode byte) error {
			switch mode % 3 {
			case 1:
				return errFuzzIssue
			case 2:
				panic("adapter panic")
			}
			return nil
		}
		supervisor, err := NewWithOptions(MustCompile(fuzzDefinition()), Options[testState, testEvent]{
			Limits: Limits{OperationTimeout: time.Second, VerificationTimeout: time.Minute, MaxRecords: 32},
			Recorder: func(_ context.Context, _ Record[testState, testEvent]) error {
				adapters.mu.Lock()
				mode := adapters.step(adapters.recorderCalls)
				adapters.recorderCalls++
				if mode%3 != 0 {
					adapters.recorderFailures++
				}
				adapters.mu.Unlock()
				return fail(mode)
			},
			Journal: func(_ context.Context, _ Snapshot[testState, testEvent]) error {
				adapters.mu.Lock()
				mode := adapters.step(adapters.journalCalls)
				adapters.journalCalls++
				adapters.mu.Unlock()
				return fail(mode)
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		ctx := context.Background()
		_, _ = supervisor.Start(ctx, &fuzzIO{})
		for range 4 {
			issued, err := supervisor.Issue(ctx, testStart, &fuzzIO{})
			if err != nil {
				if issued.Faulted && errors.Is(err, ErrJournal) && issued.IssueCompleted {
					// A failed durable preparation must have refused the Issue.
					t.Fatalf("issue completed after failed prepare: %+v", issued)
				}
				_, _ = supervisor.Recover(ctx, &fuzzIO{})
				continue
			}
			if issued.IssueCompleted {
				_, _ = supervisor.Verify(ctx, issued.AttemptKey, &fuzzIO{})
			}
			_, _ = supervisor.Recover(ctx, &fuzzIO{})
			_, _ = supervisor.Issue(ctx, testStop, &fuzzIO{})
		}

		status := supervisor.Status()
		adapters.mu.Lock()
		recorderFailures := adapters.recorderFailures
		adapters.mu.Unlock()
		if int(status.RecorderFailures) != recorderFailures {
			t.Fatalf("RecorderFailures = %d, adapter counted %d", status.RecorderFailures, recorderFailures)
		}
		if recorderFailures != 0 && status.RecorderError == "" {
			t.Fatalf("recorder failures invisible: %+v", status)
		}
		if len(supervisor.Records()) == 0 {
			t.Fatalf("in-memory records lost despite recorder failures")
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
	supervisor, err := NewWithOptions(MustCompile(benchmarkDefinition(external)), Options[testState, testEvent]{
		Limits: limits(), Unjournaled: true,
	})
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
