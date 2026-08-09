package supervised

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"sync"
	"time"
)

type pendingChange[S, E comparable, T any] struct {
	change       Change[S, E]
	transition   compiledTransition[S, E, T]
	issuedAt     time.Time
	deadline     time.Time
	timerVersion uint64
}

type activeOperation[S, E comparable] struct {
	id        uint64
	change    Change[S, E]
	phase     Phase
	ctx       context.Context
	cancel    context.CancelFunc
	startedAt time.Time
	phaseAt   time.Time
	issuedAt  time.Time
	deadline  time.Time
	callbacks int
	returned  bool
	revoked   bool
}

type faultRecord[S, E comparable] struct {
	DefinitionID         string
	ExecutionID          string
	Attempt              uint64
	Revision             uint64
	State                S
	Event                E
	TransitionID         string
	To                   S
	Phase                Phase
	Uncertain            bool
	IssuedAt             time.Time
	VerificationDeadline time.Time
	OccurredAt           time.Time
	CauseText            string
	Cause                error
}

func (f *faultRecord[S, E]) snapshot() *Fault[S, E] {
	if f == nil {
		return nil
	}
	return &Fault[S, E]{
		DefinitionID: f.DefinitionID, ExecutionID: f.ExecutionID,
		Attempt: f.Attempt, Revision: f.Revision,
		State: f.State, Event: f.Event, TransitionID: f.TransitionID,
		To: f.To, Phase: f.Phase, Uncertain: f.Uncertain,
		IssuedAt: f.IssuedAt, VerificationDeadline: f.VerificationDeadline,
		OccurredAt: f.OccurredAt,
		CauseText:  f.CauseText, Cause: f.Cause,
	}
}

// Supervisor owns one committed state under a strict Machine. It executes at
// most one callback at a time, holds at most one Change awaiting verification,
// and latches the first Fault until Recover reconciles application state.
//
// A Supervisor is safe for concurrent use and must not be copied after first
// use. It is not a safety controller: timing out or cancelling a callback does
// not terminate arbitrary Go code or prove that external effects stopped.
type Supervisor[S, E comparable, T any] struct {
	mu          sync.Mutex
	recordMu    sync.Mutex
	machine     Machine[S, E, T]
	limits      Limits
	clock       Clock
	mode        Mode
	state       S
	executionID string
	revision    uint64
	attempt     uint64

	pending         *pendingChange[S, E, T]
	fault           *faultRecord[S, E]
	operation       *activeOperation[S, E]
	nextOperation   uint64
	timer           Timer
	recorder        Recorder[S, E]
	recorderTimeout time.Duration
	journal         Journal[S, E]
	records         []Record[S, E]
	nextRecord      uint64
	recorderError   string
}

// Status is an atomic snapshot of one Supervisor's logical execution health.
// CallbackRunning can remain true after a timeout because Go cannot forcibly
// terminate the application callback.
type Status[S, E comparable] struct {
	Mode                 Mode
	Snapshot             Snapshot[S, E]
	Attempt              uint64
	Pending              *Change[S, E]
	Fault                *Fault[S, E]
	CallbackRunning      bool
	OperationRunning     bool
	Operation            uint64
	Active               Change[S, E]
	Phase                Phase
	StartedAt            time.Time
	Deadline             time.Time
	VerificationDeadline time.Time
	RecorderError        string
}

// New creates a stopped Supervisor at the Machine's canonical initial state.
// Start must reconcile application state before Issue is accepted.
func New[S, E comparable, T any](
	machine *Machine[S, E, T],
	limits Limits,
) (*Supervisor[S, E, T], error) {
	if machine == nil {
		return nil, ErrNilMachine
	}
	if err := validateLimits(limits); err != nil {
		return nil, err
	}
	executionID, err := newExecutionID()
	if err != nil {
		return nil, err
	}
	return &Supervisor[S, E, T]{
		machine:     *machine,
		limits:      limits,
		clock:       systemClock{},
		mode:        ModeStopped,
		state:       machine.initial,
		executionID: executionID,
	}, nil
}

// NewWithClock is like New and uses clock for operation and verification
// deadlines and evidentiary timestamps.
func NewWithClock[S, E comparable, T any](
	machine *Machine[S, E, T], limits Limits, clock Clock,
) (*Supervisor[S, E, T], error) {
	supervisor, err := New(machine, limits)
	if err != nil {
		return nil, err
	}
	supervisor.clock = normalizedClock(clock)
	return supervisor, nil
}

// NewWithOptions constructs a Supervisor with explicit clock and Recorder seams.
func NewWithOptions[S, E comparable, T any](
	machine *Machine[S, E, T], options Options[S, E],
) (*Supervisor[S, E, T], error) {
	supervisor, err := New(machine, options.Limits)
	if err != nil {
		return nil, err
	}
	supervisor.clock = normalizedClock(options.Clock)
	supervisor.recorder = options.Recorder
	supervisor.recorderTimeout = options.RecorderTimeout
	if supervisor.recorderTimeout <= 0 {
		supervisor.recorderTimeout = options.Limits.OperationTimeout
	}
	if options.RequireJournal && options.Journal == nil {
		return nil, ErrJournalRequired
	}
	supervisor.journal = options.Journal
	return supervisor, nil
}

// Restore creates a stopped Supervisor from a logical Snapshot. The
// definition ID and state are validated, but Start must still reconcile the
// Snapshot with physical and durable application state.
func Restore[S, E comparable, T any](
	machine *Machine[S, E, T],
	snapshot Snapshot[S, E],
	limits Limits,
) (*Supervisor[S, E, T], error) {
	if machine == nil {
		return nil, ErrNilMachine
	}
	if err := validateLimits(limits); err != nil {
		return nil, err
	}
	if snapshot.DefinitionID != machine.id {
		return nil, ErrDefinitionMismatch
	}
	if snapshot.ExecutionID == "" {
		return nil, ErrInvalidSnapshot
	}
	if _, known := machine.stateIndex[snapshot.State]; !known {
		return nil, ErrUnknownState
	}
	if snapshot.Revision == math.MaxUint64 {
		return nil, ErrCounterExhausted
	}
	if snapshot.InDoubt != (snapshot.Pending != nil) {
		return nil, ErrInvalidSnapshot
	}
	if pending := snapshot.Pending; pending != nil {
		if pending.Attempt.ExecutionID != snapshot.ExecutionID || pending.Attempt.Sequence == 0 ||
			pending.Attempt.Sequence > snapshot.Attempt || pending.Revision != snapshot.Revision ||
			pending.From != snapshot.State {
			return nil, ErrInvalidSnapshot
		}
		matched := false
		for _, transition := range machine.rows[transitionKey[S, E]{pending.From, pending.Event}] {
			if transition.id == pending.TransitionID && transition.to == pending.To && transition.issue != nil {
				matched = true
				break
			}
		}
		if !matched {
			return nil, ErrInvalidSnapshot
		}
	}
	supervisor := &Supervisor[S, E, T]{
		machine:     *machine,
		limits:      limits,
		clock:       systemClock{},
		mode:        ModeStopped,
		state:       snapshot.State,
		revision:    snapshot.Revision,
		attempt:     snapshot.Attempt,
		executionID: snapshot.ExecutionID,
	}
	if snapshot.InDoubt || snapshot.Faulted {
		cause := ErrRestoredInDoubt
		if snapshot.FaultCause != "" {
			cause = errors.New(snapshot.FaultCause)
		}
		supervisor.mode = ModeFaulted
		supervisor.fault = &faultRecord[S, E]{
			DefinitionID: machine.id, ExecutionID: snapshot.ExecutionID,
			Attempt: snapshot.Attempt, Revision: snapshot.Revision,
			State: snapshot.State, Phase: snapshot.FaultPhase,
			Uncertain:  snapshot.Uncertain || snapshot.InDoubt,
			OccurredAt: snapshot.RecordedAt, CauseText: cause.Error(), Cause: cause,
		}
		if pending := snapshot.Pending; pending != nil {
			supervisor.fault.Attempt = pending.Attempt.Sequence
			supervisor.fault.Event = pending.Event
			supervisor.fault.TransitionID = pending.TransitionID
			supervisor.fault.To = pending.To
			supervisor.fault.IssuedAt = pending.IssuedAt
			supervisor.fault.VerificationDeadline = pending.VerificationDeadline
		}
	}
	return supervisor, nil
}

func newExecutionID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("supervised: create execution identity: %w", err)
	}
	return hex.EncodeToString(bytes[:]), nil
}

// RestoreWithClock is like Restore and uses clock for verification deadlines.
func RestoreWithClock[S, E comparable, T any](
	machine *Machine[S, E, T], snapshot Snapshot[S, E], limits Limits, clock Clock,
) (*Supervisor[S, E, T], error) {
	supervisor, err := Restore(machine, snapshot, limits)
	if err != nil {
		return nil, err
	}
	supervisor.clock = normalizedClock(clock)
	return supervisor, nil
}

// RestoreWithOptions is like Restore with explicit clock and Recorder seams.
func RestoreWithOptions[S, E comparable, T any](
	machine *Machine[S, E, T], snapshot Snapshot[S, E], options Options[S, E],
) (*Supervisor[S, E, T], error) {
	supervisor, err := Restore(machine, snapshot, options.Limits)
	if err != nil {
		return nil, err
	}
	supervisor.clock = normalizedClock(options.Clock)
	supervisor.recorder = options.Recorder
	supervisor.recorderTimeout = options.RecorderTimeout
	if supervisor.recorderTimeout <= 0 {
		supervisor.recorderTimeout = options.Limits.OperationTimeout
	}
	if options.RequireJournal && options.Journal == nil {
		return nil, ErrJournalRequired
	}
	supervisor.journal = options.Journal
	return supervisor, nil
}

func validateLimits(limits Limits) error {
	if limits.OperationTimeout <= 0 || limits.VerificationTimeout <= 0 {
		return ErrInvalidLimits
	}
	return nil
}

// Start runs every Reconciler against the initial or restored Snapshot. A
// failure latches a Fault; success moves the Supervisor to Ready.
func (s *Supervisor[S, E, T]) Start(ctx context.Context, data T) (Result[S, E], error) {
	result := s.start(ctx, data)
	s.completeResult(&result)
	s.recordResult(RecordStart, result)
	return result, result.Err
}

func (s *Supervisor[S, E, T]) start(ctx context.Context, data T) Result[S, E] {
	if ctx == nil {
		return s.statusResult(PhaseStart, ErrNilContext)
	}

	s.mu.Lock()
	base := s.baseResultLocked()
	switch s.mode {
	case ModeStopped:
	case ModeFaulted:
		base.Faulted = true
		base.Uncertain = s.fault.Uncertain
		base.Err = s.fault.snapshot()
		s.mu.Unlock()
		return base
	case ModeExecuting:
		base.Err = ErrBusy
		s.mu.Unlock()
		return base
	default:
		base.Err = ErrAlreadyStarted
		s.mu.Unlock()
		return base
	}
	change := s.currentChangeLocked()
	op, opCtx := s.beginOperationLocked(ctx, change, PhaseStart)
	base.StartedAt = op.startedAt
	base.Operation = op.id
	snapshot := s.snapshotLocked()
	s.mu.Unlock()
	defer s.operationReturned(op)

	for _, reconcile := range s.machine.reconcile {
		outcome := s.invoke(op, opCtx, change, PhaseStart, func(callCtx context.Context) error {
			return reconcile(callCtx, snapshot, data)
		})
		if outcome.err != nil {
			cause := outcome.err
			if outcome.completed {
				cause = &ViolationError{Phase: PhaseStart, Reason: cause}
			}
			return s.faultResult(base, s.latch(op, change, PhaseStart, cause, true))
		}
	}
	if err := opCtx.Err(); err != nil {
		return s.faultResult(base, s.latch(op, change, PhaseStart, contextFailure(opCtx, err), true))
	}
	if fault := s.finishReady(op); fault != nil {
		return s.faultResult(base, fault)
	}
	base.Phase = PhaseStart
	return base
}

// Issue accepts one event, runs non-bypassable Preconditions, selects one
// guarded row, validates Invariants, and invokes its Issue action.
//
// A purely logical row has no Issue or Verify and commits before Issue returns.
// An external row returns IssueCompleted with Committed false and must be
// completed by Verify using the returned Attempt identifier.
func (s *Supervisor[S, E, T]) Issue(ctx context.Context, event E, data T) (Result[S, E], error) {
	result := s.issue(ctx, event, data)
	s.completeResult(&result)
	s.recordResult(RecordIssue, result)
	return result, result.Err
}

func (s *Supervisor[S, E, T]) issue(ctx context.Context, event E, data T) Result[S, E] {
	if ctx == nil {
		result := s.statusResult(PhaseNone, ErrNilContext)
		result.Event = event
		return result
	}

	s.mu.Lock()
	base := s.baseResultLocked()
	base.Event = event
	switch s.mode {
	case ModeStopped:
		base.Err = ErrNotStarted
		s.mu.Unlock()
		return base
	case ModeExecuting:
		base.Err = ErrBusy
		s.mu.Unlock()
		return base
	case ModeAwaitingVerification:
		base.Err = ErrAwaitingVerification
		s.mu.Unlock()
		return base
	case ModeFaulted:
		base.Faulted = true
		base.Uncertain = s.fault.Uncertain
		base.Err = s.fault.snapshot()
		s.mu.Unlock()
		return base
	case ModeReady:
	}
	if _, known := s.machine.eventSet[event]; !known {
		base.Phase = PhaseSelection
		base.Err = ErrUnknownEvent
		s.mu.Unlock()
		return base
	}
	if s.attempt == math.MaxUint64 || s.revision == math.MaxUint64 {
		change := s.currentChangeLocked()
		change.Event = event
		op, _ := s.beginOperationLocked(ctx, change, PhaseSelection)
		s.mu.Unlock()
		defer s.operationReturned(op)
		return s.faultResult(base, s.latch(op, change, PhaseSelection, ErrCounterExhausted, false))
	}
	s.attempt++
	attempt := Attempt[S, E]{
		DefinitionID: s.machine.id,
		ExecutionID:  s.executionID,
		ID:           s.attempt,
		Revision:     s.revision,
		StartedAt:    s.clock.Now(),
		From:         s.state,
		Event:        event,
	}
	change := Change[S, E]{Attempt: attempt, To: s.state}
	base.Attempt = attempt.ID
	base.AttemptKey = attempt.Identifier()
	op, opCtx := s.beginOperationLocked(ctx, change, PhasePrecondition)
	base.StartedAt = op.startedAt
	base.Operation = op.id
	s.mu.Unlock()
	defer s.operationReturned(op)

	for _, precondition := range s.machine.preconditions {
		outcome := s.invoke(op, opCtx, change, PhasePrecondition, func(callCtx context.Context) error {
			return precondition(callCtx, attempt, data)
		})
		if outcome.err != nil {
			cause := outcome.err
			if outcome.completed {
				cause = &ViolationError{Phase: PhasePrecondition, Reason: cause}
			}
			return s.faultResult(base, s.latch(op, change, PhasePrecondition, cause, false))
		}
	}

	var selected compiledTransition[S, E, T]
	var reasons []error
	found := false
	for _, candidate := range s.machine.rows[transitionKey[S, E]{attempt.From, event}] {
		candidateChange := Change[S, E]{
			Attempt:      attempt,
			TransitionID: candidate.id,
			To:           candidate.to,
		}
		if candidate.guard != nil {
			outcome := s.invoke(op, opCtx, candidateChange, PhaseGuard, func(callCtx context.Context) error {
				return candidate.guard(callCtx, candidateChange, data)
			})
			if outcome.err != nil {
				if !outcome.completed {
					return s.faultResult(base, s.latch(op, candidateChange, PhaseGuard, outcome.err, false))
				}
				reasons = append(reasons, outcome.err)
				continue
			}
		}
		selected = candidate
		change = candidateChange
		found = true
		break
	}
	if !found {
		if err := opCtx.Err(); err != nil {
			return s.faultResult(base, s.latch(op, change, PhaseSelection, contextFailure(opCtx, err), false))
		}
		if fault := s.finishReady(op); fault != nil {
			return s.faultResult(base, fault)
		}
		base.Phase = PhaseSelection
		base.Err = &refusal[S, E]{
			from:   attempt.From,
			event:  event,
			unwrap: append([]error{ErrNotPermitted}, reasons...),
		}
		return base
	}
	base.Selected = true
	base.TransitionID = change.TransitionID
	base.To = change.To

	if result, stopped := s.runChecks(op, opCtx, base, change, PhasePrecondition, selected.preconditions, data, false); stopped {
		return result
	}
	if result, stopped := s.runChecks(op, opCtx, base, change, PhaseInvariant, s.machine.invariants, data, false); stopped {
		return result
	}

	if selected.issue == nil {
		if result, stopped := s.runChecks(op, opCtx, base, change, PhasePostcondition, s.machine.postconditions, data, false); stopped {
			return result
		}
		return s.commit(op, opCtx, base, change, false, false)
	}
	if err := s.prepareIssue(op, opCtx, change); err != nil {
		return s.faultResult(base, s.latch(op, change, PhaseIssue, err, false))
	}

	outcome := s.invoke(op, opCtx, change, PhaseIssue, func(callCtx context.Context) error {
		return selected.issue(callCtx, change, data)
	})
	if outcome.err != nil {
		return s.faultResult(base, s.latch(op, change, PhaseIssue, outcome.err, true))
	}
	base.IssueCompleted = true
	base.Phase = PhaseIssue
	if err := opCtx.Err(); err != nil {
		return s.faultResult(base, s.latch(op, change, PhaseIssue, contextFailure(opCtx, err), true))
	}
	if fault := s.awaitVerification(op, change, selected); fault != nil {
		return s.faultResult(base, fault)
	}
	return base
}

func (s *Supervisor[S, E, T]) prepareIssue(
	op *activeOperation[S, E], ctx context.Context, change Change[S, E],
) error {
	s.mu.Lock()
	if !s.ownsLocked(op) || s.mode != ModeExecuting {
		fault := s.fault.snapshot()
		s.mu.Unlock()
		if fault != nil {
			return fault
		}
		return ErrBusy
	}
	op.phase = PhaseIssue
	op.phaseAt = s.clock.Now()
	op.issuedAt = op.phaseAt
	op.change = change
	snapshot := s.snapshotLocked()
	journal := s.journal
	s.mu.Unlock()
	if journal == nil {
		return nil
	}
	outcome := s.invoke(op, ctx, change, PhaseIssue, func(callCtx context.Context) error {
		return journal(callCtx, snapshot)
	})
	if outcome.err != nil {
		return errors.Join(ErrJournal, outcome.err)
	}
	return nil
}

// Verify completes an issued Change using fresh application data. Verify,
// Invariants, and Postconditions must all succeed before logical commit.
func (s *Supervisor[S, E, T]) Verify(ctx context.Context, attempt AttemptID, data T) (Result[S, E], error) {
	result := s.verify(ctx, attempt, data)
	s.completeResult(&result)
	s.recordResult(RecordVerify, result)
	return result, result.Err
}

func (s *Supervisor[S, E, T]) verify(ctx context.Context, attempt AttemptID, data T) Result[S, E] {
	if ctx == nil {
		return s.statusResult(PhaseVerify, ErrNilContext)
	}

	s.mu.Lock()
	base := s.baseResultLocked()
	switch s.mode {
	case ModeStopped:
		base.Err = ErrNotStarted
		s.mu.Unlock()
		return base
	case ModeReady:
		base.Err = ErrNoPending
		s.mu.Unlock()
		return base
	case ModeExecuting:
		base.Err = ErrBusy
		s.mu.Unlock()
		return base
	case ModeFaulted:
		base.Faulted = true
		base.Uncertain = s.fault.Uncertain
		base.Err = s.fault.snapshot()
		s.mu.Unlock()
		return base
	case ModeAwaitingVerification:
	}
	if s.pending == nil {
		base.Err = ErrNoPending
		s.mu.Unlock()
		return base
	}
	if s.pending.change.Identifier() != attempt {
		base.Attempt = attempt.Sequence
		base.AttemptKey = attempt
		base.Err = ErrStaleAttempt
		s.mu.Unlock()
		return base
	}
	if !s.clock.Now().Before(s.pending.deadline) {
		change := s.pending.change
		fault, cancel := s.latchLocked(nil, change, PhaseVerify, ErrVerificationTimeout, true)
		s.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return s.faultResult(base, fault)
	}
	pending := *s.pending
	change := pending.change
	base = resultFromChange(change)
	base.Revision = s.revision
	base.IssueCompleted = true
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	op, opCtx := s.beginOperationLocked(ctx, change, PhaseVerify)
	base.StartedAt = op.startedAt
	base.Operation = op.id
	s.mu.Unlock()
	defer s.operationReturned(op)

	outcome := s.invoke(op, opCtx, change, PhaseVerify, func(callCtx context.Context) error {
		return pending.transition.verify(callCtx, change, data)
	})
	if outcome.err != nil {
		cause := outcome.err
		if outcome.completed {
			cause = &ViolationError{Phase: PhaseVerify, Reason: cause}
		}
		return s.faultResult(base, s.latch(op, change, PhaseVerify, cause, true))
	}
	base.Verified = true
	if result, stopped := s.runChecks(op, opCtx, base, change, PhaseInvariant, s.machine.invariants, data, true); stopped {
		return result
	}
	if result, stopped := s.runChecks(op, opCtx, base, change, PhasePostcondition, s.machine.postconditions, data, true); stopped {
		return result
	}
	return s.commit(op, opCtx, base, change, true, true)
}

// Trip latches the first external or supervisory reason. It cancels the
// current callback context and prevents logical commit, but cannot terminate a
// callback that ignores cancellation or stop external hardware.
func (s *Supervisor[S, E, T]) Trip(reason error) Fault[S, E] {
	if reason == nil {
		reason = ErrTripped
	}
	s.mu.Lock()
	change := s.currentChangeLocked()
	phase := PhaseTrip
	if s.mode == ModeExecuting {
		change = s.operation.change
		phase = s.operation.phase
	} else if s.mode == ModeAwaitingVerification && s.pending != nil {
		change = s.pending.change
		phase = PhaseVerify
	}
	// An external Trip conservatively records physical uncertainty even when no
	// Issue is active: the caller may be reporting unexpected plant behavior.
	fault, cancel := s.latchLocked(nil, change, phase, reason, true)
	copy := *fault
	result := resultFromChange(change)
	result.Operation = 0
	if s.operation != nil {
		result.Operation = s.operation.id
	}
	result.Phase = copy.Phase
	result.Faulted = true
	result.Uncertain = copy.Uncertain
	result.Err = reason
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.completeResult(&result)
	s.recordResult(RecordTrip, result)
	return copy
}

// Recover runs every Reconciler against the last committed Snapshot. Recovery
// is refused while the prior Operation call or any callback it started remains
// live. A failed recovery retains the original first-cause Fault.
func (s *Supervisor[S, E, T]) Recover(ctx context.Context, data T) (Result[S, E], error) {
	result := s.recover(ctx, data)
	s.completeResult(&result)
	s.recordResult(RecordRecover, result)
	return result, result.Err
}

func (s *Supervisor[S, E, T]) recover(ctx context.Context, data T) Result[S, E] {
	if ctx == nil {
		return s.statusResult(PhaseRecover, ErrNilContext)
	}

	s.mu.Lock()
	base := s.baseResultLocked()
	if s.mode != ModeFaulted {
		base.Err = ErrNotFaulted
		s.mu.Unlock()
		return base
	}
	if s.operation != nil {
		base.Faulted = true
		base.Uncertain = s.fault.Uncertain
		base.Err = ErrCallbackRunning
		s.mu.Unlock()
		return base
	}
	original := s.fault
	originalFault := original.snapshot()
	change := s.currentChangeLocked()
	op, opCtx := s.beginOperationLocked(ctx, change, PhaseRecover)
	base.StartedAt = op.startedAt
	base.Operation = op.id
	snapshot := s.snapshotLocked()
	s.mu.Unlock()
	defer s.operationReturned(op)

	for _, reconcile := range s.machine.reconcile {
		outcome := s.invoke(op, opCtx, change, PhaseRecover, func(callCtx context.Context) error {
			return reconcile(callCtx, snapshot, data)
		})
		if outcome.err != nil {
			cause := outcome.err
			if outcome.completed {
				cause = &ViolationError{Phase: PhaseRecover, Reason: cause}
			}
			s.restoreFault(op, original)
			base.Phase = PhaseRecover
			base.Faulted = true
			base.Uncertain = original.Uncertain
			base.Err = errors.Join(originalFault, cause)
			return base
		}
	}
	if err := opCtx.Err(); err != nil {
		s.restoreFault(op, original)
		base.Phase = PhaseRecover
		base.Faulted = true
		base.Uncertain = original.Uncertain
		base.Err = errors.Join(originalFault, contextFailure(opCtx, err))
		return base
	}

	s.mu.Lock()
	if !s.ownsLocked(op) || s.mode != ModeExecuting || s.fault != original {
		fault := s.fault.snapshot()
		s.mu.Unlock()
		return s.faultResult(base, fault)
	}
	cancel := op.cancel
	op.cancel = nil
	s.fault = nil
	s.mode = ModeReady
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	base.Phase = PhaseRecover
	return base
}

// Snapshot returns the last committed logical state and revision.
func (s *Supervisor[S, E, T]) Snapshot() Snapshot[S, E] {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

// Status returns one atomic execution-health snapshot.
func (s *Supervisor[S, E, T]) Status() Status[S, E] {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := Status[S, E]{
		Mode:     s.mode,
		Snapshot: s.snapshotLocked(),
		Attempt:  s.attempt,
	}
	if s.operation != nil {
		status.CallbackRunning = s.operation.callbacks != 0
		status.OperationRunning = true
		status.Operation = s.operation.id
		status.Active = s.operation.change
		status.Phase = s.operation.phase
		status.StartedAt = s.operation.startedAt
		status.Deadline = s.operation.deadline
	}
	if s.pending != nil {
		pending := s.pending.change
		status.Pending = &pending
		status.VerificationDeadline = s.pending.deadline
	}
	status.RecorderError = s.recorderError
	if s.fault != nil {
		status.Fault = s.fault.snapshot()
	}
	return status
}

func (s *Supervisor[S, E, T]) runChecks(
	op *activeOperation[S, E],
	ctx context.Context,
	base Result[S, E],
	change Change[S, E],
	phase Phase,
	checks []Check[S, E, T],
	data T,
	uncertain bool,
) (Result[S, E], bool) {
	for _, check := range checks {
		outcome := s.invoke(op, ctx, change, phase, func(callCtx context.Context) error {
			return check(callCtx, change, data)
		})
		if outcome.err == nil {
			continue
		}
		cause := outcome.err
		if outcome.completed {
			cause = &ViolationError{Phase: phase, Reason: cause}
		}
		return s.faultResult(base, s.latch(op, change, phase, cause, uncertain)), true
	}
	return base, false
}

type callbackOutcome struct {
	err       error
	completed bool
}

func (s *Supervisor[S, E, T]) invoke(
	op *activeOperation[S, E],
	ctx context.Context,
	change Change[S, E],
	phase Phase,
	callback func(context.Context) error,
) callbackOutcome {
	if err := ctx.Err(); err != nil {
		return callbackOutcome{err: contextFailure(ctx, err)}
	}
	s.mu.Lock()
	if !s.ownsLocked(op) || s.mode != ModeExecuting {
		fault := s.fault.snapshot()
		s.mu.Unlock()
		if fault != nil {
			return callbackOutcome{err: fault}
		}
		return callbackOutcome{err: ErrBusy}
	}
	op.change = change
	op.phase = phase
	op.phaseAt = s.clock.Now()
	op.callbacks++
	s.mu.Unlock()

	done := make(chan callbackOutcome, 1)
	go func() {
		outcome := callbackOutcome{}
		returned := false
		defer func() {
			if !returned {
				if recovered := recover(); recovered != nil {
					outcome.err = &PanicError{Value: recovered, Stack: string(debug.Stack())}
				} else {
					outcome.err = ErrExecutionStopped
				}
			} else {
				outcome.completed = true
			}
			done <- outcome
			s.mu.Lock()
			op.callbacks--
			lateCause := op.revoked && outcome.err != nil
			if s.operation == op && op.returned && op.callbacks == 0 {
				s.operation = nil
			}
			s.mu.Unlock()
			if lateCause {
				result := resultFromChange(change)
				result.Operation = op.id
				result.Phase = phase
				result.Faulted = true
				result.Uncertain = phase == PhaseIssue || phase == PhaseVerify || phase == PhasePostcondition
				result.Err = outcome.err
				s.completeResult(&result)
				s.recordResult(RecordSecondaryCause, result)
			}
		}()
		outcome.err = callback(ctx)
		returned = true
	}()

	select {
	case outcome := <-done:
		if err := ctx.Err(); err != nil {
			timeout := contextFailure(ctx, err)
			if outcome.err != nil {
				outcome.err = errors.Join(outcome.err, timeout)
				return outcome
			}
			return callbackOutcome{err: timeout, completed: outcome.completed}
		}
		return outcome
	case <-ctx.Done():
		// Prefer an outcome that was already published when the deadline became
		// observable. This preserves a mandatory-check violation instead of
		// substituting a generic timeout when both facts are true.
		select {
		case outcome := <-done:
			timeout := contextFailure(ctx, ctx.Err())
			if outcome.err != nil {
				outcome.err = errors.Join(outcome.err, timeout)
				return outcome
			}
			return callbackOutcome{err: timeout, completed: outcome.completed}
		default:
			return callbackOutcome{err: contextFailure(ctx, ctx.Err())}
		}
	}
}

func contextFailure(ctx context.Context, err error) error {
	if errors.Is(context.Cause(ctx), ErrOperationTimeout) {
		return &operationTimeoutError{cause: context.DeadlineExceeded}
	}
	return err
}

func (s *Supervisor[S, E, T]) awaitVerification(
	op *activeOperation[S, E],
	change Change[S, E],
	transition compiledTransition[S, E, T],
) *Fault[S, E] {
	s.mu.Lock()
	if !s.ownsLocked(op) || s.mode != ModeExecuting {
		fault := s.fault.snapshot()
		s.mu.Unlock()
		return fault
	}
	cancel := op.cancel
	op.cancel = nil
	deadline := s.clock.Now().Add(s.limits.VerificationTimeout)
	s.pending = &pendingChange[S, E, T]{
		change: change, transition: transition, issuedAt: op.issuedAt,
		deadline: deadline, timerVersion: op.id,
	}
	s.mode = ModeAwaitingVerification
	s.timer = s.clock.AfterFunc(s.limits.VerificationTimeout, func() {
		s.verificationExpired(change.ID, op.id)
	})
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

func (s *Supervisor[S, E, T]) verificationExpired(attempt, timerVersion uint64) {
	s.mu.Lock()
	if s.mode != ModeAwaitingVerification || s.pending == nil ||
		s.pending.change.ID != attempt || s.pending.timerVersion != timerVersion {
		s.mu.Unlock()
		return
	}
	now := s.clock.Now()
	if now.Before(s.pending.deadline) {
		remaining := s.pending.deadline.Sub(now)
		s.timer = s.clock.AfterFunc(remaining, func() { s.verificationExpired(attempt, timerVersion) })
		s.mu.Unlock()
		return
	}
	change := s.pending.change
	fault, cancel := s.latchLocked(nil, change, PhaseVerify, ErrVerificationTimeout, true)
	result := resultFromChange(change)
	result.Phase = PhaseVerify
	result.IssueCompleted = true
	result.Faulted = true
	result.Uncertain = true
	result.Err = fault
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.completeResult(&result)
	s.recordResult(RecordVerificationExpired, result)
}

func (s *Supervisor[S, E, T]) commit(
	op *activeOperation[S, E],
	ctx context.Context,
	base Result[S, E],
	change Change[S, E],
	issueCompleted bool,
	verified bool,
) Result[S, E] {
	if err := ctx.Err(); err != nil {
		return s.faultResult(base, s.latch(op, change, PhaseCommit, contextFailure(ctx, err), issueCompleted))
	}
	s.mu.Lock()
	if !s.ownsLocked(op) || s.mode != ModeExecuting {
		fault := s.fault.snapshot()
		s.mu.Unlock()
		return s.faultResult(base, fault)
	}
	if s.revision == math.MaxUint64 {
		fault, cancel := s.latchLocked(op, change, PhaseCommit, ErrCounterExhausted, issueCompleted)
		s.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return s.faultResult(base, fault)
	}
	s.state = change.To
	s.revision++
	s.pending = nil
	cancel := op.cancel
	op.cancel = nil
	s.mode = ModeReady
	revision := s.revision
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	base.Phase = PhaseCommit
	base.Revision = revision
	base.IssueCompleted = issueCompleted
	base.Verified = verified
	base.Committed = true
	return base
}

func (s *Supervisor[S, E, T]) beginOperationLocked(
	parent context.Context,
	change Change[S, E],
	phase Phase,
) (*activeOperation[S, E], context.Context) {
	ctx, cancelCause := context.WithCancelCause(parent)
	timer := s.clock.AfterFunc(s.limits.OperationTimeout, func() {
		cancelCause(ErrOperationTimeout)
	})
	cancel := func() {
		if timer != nil {
			timer.Stop()
		}
		cancelCause(context.Canceled)
	}
	startedAt := s.clock.Now()
	s.nextOperation++
	op := &activeOperation[S, E]{
		id: s.nextOperation, change: change, phase: phase, ctx: ctx, cancel: cancel,
		startedAt: startedAt, phaseAt: startedAt, deadline: startedAt.Add(s.limits.OperationTimeout),
	}
	s.mode = ModeExecuting
	s.operation = op
	return op, ctx
}

func (s *Supervisor[S, E, T]) ownsLocked(op *activeOperation[S, E]) bool {
	return op != nil && s.operation == op && !op.revoked
}

func (s *Supervisor[S, E, T]) operationReturned(op *activeOperation[S, E]) {
	s.mu.Lock()
	op.returned = true
	if s.operation == op && op.callbacks == 0 {
		s.operation = nil
	}
	s.mu.Unlock()
}

func (s *Supervisor[S, E, T]) finishReady(op *activeOperation[S, E]) *Fault[S, E] {
	s.mu.Lock()
	if !s.ownsLocked(op) || s.mode != ModeExecuting {
		fault := s.fault.snapshot()
		s.mu.Unlock()
		return fault
	}
	cancel := op.cancel
	op.cancel = nil
	s.mode = ModeReady
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

func (s *Supervisor[S, E, T]) restoreFault(op *activeOperation[S, E], fault *faultRecord[S, E]) {
	s.mu.Lock()
	if !s.ownsLocked(op) {
		s.mu.Unlock()
		return
	}
	cancel := op.cancel
	op.cancel = nil
	op.revoked = true
	s.fault = fault
	s.mode = ModeFaulted
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *Supervisor[S, E, T]) latch(
	op *activeOperation[S, E],
	change Change[S, E],
	phase Phase,
	cause error,
	uncertain bool,
) *Fault[S, E] {
	s.mu.Lock()
	fault, cancel := s.latchLocked(op, change, phase, cause, uncertain)
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return fault
}

func (s *Supervisor[S, E, T]) latchLocked(
	op *activeOperation[S, E],
	change Change[S, E],
	phase Phase,
	cause error,
	uncertain bool,
) (*Fault[S, E], context.CancelFunc) {
	if op != nil && !s.ownsLocked(op) {
		return s.fault.snapshot(), nil
	}
	if s.fault == nil {
		var issuedAt, verificationDeadline time.Time
		if s.pending != nil && s.pending.change.Identifier() == change.Identifier() {
			issuedAt = s.pending.issuedAt
			verificationDeadline = s.pending.deadline
		} else if s.operation != nil && s.operation.change.Identifier() == change.Identifier() {
			issuedAt = s.operation.issuedAt
		}
		s.fault = &faultRecord[S, E]{
			DefinitionID:         s.machine.id,
			ExecutionID:          s.executionID,
			Attempt:              change.ID,
			Revision:             s.revision,
			State:                s.state,
			Event:                change.Event,
			TransitionID:         change.TransitionID,
			To:                   change.To,
			Phase:                phase,
			Uncertain:            uncertain,
			IssuedAt:             issuedAt,
			VerificationDeadline: verificationDeadline,
			OccurredAt:           s.clock.Now(),
			CauseText:            fmt.Sprint(cause),
			Cause:                cause,
		}
	}
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	s.pending = nil
	var cancel context.CancelFunc
	if s.operation != nil {
		s.operation.revoked = true
		cancel = s.operation.cancel
		s.operation.cancel = nil
	}
	s.mode = ModeFaulted
	return s.fault.snapshot(), cancel
}

func (s *Supervisor[S, E, T]) snapshotLocked() Snapshot[S, E] {
	snapshot := Snapshot[S, E]{
		DefinitionID: s.machine.id,
		ExecutionID:  s.executionID,
		State:        s.state,
		Revision:     s.revision,
		Attempt:      s.attempt,
		RecordedAt:   s.clock.Now(),
	}
	if s.pending != nil {
		snapshot.InDoubt = true
		snapshot.Uncertain = true
		snapshot.Pending = &PendingSnapshot[S, E]{
			Attempt: s.pending.change.Identifier(), Revision: s.pending.change.Revision,
			From: s.pending.change.From, Event: s.pending.change.Event,
			TransitionID: s.pending.change.TransitionID, To: s.pending.change.To,
			IssuedAt: s.pending.issuedAt, VerificationDeadline: s.pending.deadline,
		}
	}
	if s.operation != nil && !s.operation.issuedAt.IsZero() {
		snapshot.InDoubt = true
		snapshot.Uncertain = true
		if snapshot.Pending == nil && s.operation.change.ID != 0 {
			snapshot.Pending = &PendingSnapshot[S, E]{
				Attempt: s.operation.change.Identifier(), Revision: s.operation.change.Revision,
				From: s.operation.change.From, Event: s.operation.change.Event,
				TransitionID: s.operation.change.TransitionID, To: s.operation.change.To,
				IssuedAt: s.operation.issuedAt,
			}
		}
	}
	if s.fault != nil {
		snapshot.Faulted = true
		snapshot.Uncertain = s.fault.Uncertain
		snapshot.FaultPhase = s.fault.Phase
		snapshot.FaultCause = s.fault.CauseText
		if snapshot.Pending == nil && !s.fault.IssuedAt.IsZero() {
			snapshot.InDoubt = true
			snapshot.Pending = &PendingSnapshot[S, E]{
				Attempt:  AttemptID{ExecutionID: s.fault.ExecutionID, Sequence: s.fault.Attempt},
				Revision: s.fault.Revision, From: s.fault.State, Event: s.fault.Event,
				TransitionID: s.fault.TransitionID, To: s.fault.To,
				IssuedAt: s.fault.IssuedAt, VerificationDeadline: s.fault.VerificationDeadline,
			}
		}
	}
	return snapshot
}

func (s *Supervisor[S, E, T]) currentChangeLocked() Change[S, E] {
	return Change[S, E]{
		Attempt: Attempt[S, E]{
			DefinitionID: s.machine.id,
			ExecutionID:  s.executionID,
			ID:           s.attempt,
			Revision:     s.revision,
			From:         s.state,
		},
		To: s.state,
	}
}

func (s *Supervisor[S, E, T]) baseResultLocked() Result[S, E] {
	return Result[S, E]{
		DefinitionID: s.machine.id,
		ExecutionID:  s.executionID,
		Attempt:      s.attempt,
		AttemptKey:   AttemptID{ExecutionID: s.executionID, Sequence: s.attempt},
		Revision:     s.revision,
		From:         s.state,
		To:           s.state,
	}
}

func (s *Supervisor[S, E, T]) statusResult(phase Phase, err error) Result[S, E] {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := s.baseResultLocked()
	result.Phase = phase
	result.Err = err
	if s.mode == ModeFaulted && s.fault != nil {
		result.Faulted = true
		result.Uncertain = s.fault.Uncertain
	}
	return result
}

func (s *Supervisor[S, E, T]) completeResult(result *Result[S, E]) {
	if result.CompletedAt.IsZero() {
		result.CompletedAt = s.clock.Now()
	}
	if result.StartedAt.IsZero() {
		result.StartedAt = result.CompletedAt
	}
}

func resultFromChange[S, E comparable](change Change[S, E]) Result[S, E] {
	return Result[S, E]{
		DefinitionID: change.DefinitionID,
		ExecutionID:  change.ExecutionID,
		Attempt:      change.ID,
		AttemptKey:   change.Identifier(),
		Revision:     change.Revision,
		StartedAt:    change.StartedAt,
		From:         change.From,
		Event:        change.Event,
		TransitionID: change.TransitionID,
		To:           change.To,
		Selected:     change.TransitionID != "",
	}
}

func (s *Supervisor[S, E, T]) faultResult(
	base Result[S, E],
	fault *Fault[S, E],
) Result[S, E] {
	if fault == nil {
		base.Faulted = true
		base.Err = ErrFaulted
		return base
	}
	base.Phase = fault.Phase
	base.Revision = fault.Revision
	base.Faulted = true
	base.Uncertain = fault.Uncertain
	base.Err = fault
	return base
}
