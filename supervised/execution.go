package supervised

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

type pendingChange[S, E comparable, T any] struct {
	change     Change[S, E]
	transition compiledTransition[S, E, T]
}

type activeOperation[S, E comparable] struct {
	change Change[S, E]
	phase  Phase
}

// Supervisor owns one committed state under a strict Machine. It executes at
// most one callback at a time, holds at most one Change awaiting verification,
// and latches the first Fault until Recover reconciles application state.
//
// A Supervisor is safe for concurrent use and must not be copied after first
// use. It is not a safety controller: timing out or cancelling a callback does
// not terminate arbitrary Go code or prove that external effects stopped.
type Supervisor[S, E comparable, T any] struct {
	mu       sync.Mutex
	machine  *Machine[S, E, T]
	limits   Limits
	mode     Mode
	state    S
	revision uint64
	attempt  uint64

	pending *pendingChange[S, E, T]
	fault   *Fault[S, E]
	active  activeOperation[S, E]
	cancel  context.CancelFunc
	timer   *time.Timer

	callbacks int
}

// Status is an atomic snapshot of one Supervisor's logical execution health.
// CallbackRunning can remain true after a timeout because Go cannot forcibly
// terminate the application callback.
type Status[S, E comparable] struct {
	Mode            Mode
	Snapshot        Snapshot[S]
	Attempt         uint64
	Pending         *Change[S, E]
	Fault           *Fault[S, E]
	CallbackRunning bool
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
	return &Supervisor[S, E, T]{
		machine: machine,
		limits:  limits,
		mode:    ModeStopped,
		state:   machine.initial,
	}, nil
}

// Restore creates a stopped Supervisor from a logical Snapshot. The
// definition ID and state are validated, but Start must still reconcile the
// Snapshot with physical and durable application state.
func Restore[S, E comparable, T any](
	machine *Machine[S, E, T],
	snapshot Snapshot[S],
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
	if _, known := machine.stateIndex[snapshot.State]; !known {
		return nil, ErrUnknownState
	}
	if snapshot.Revision == math.MaxUint64 {
		return nil, ErrCounterExhausted
	}
	return &Supervisor[S, E, T]{
		machine:  machine,
		limits:   limits,
		mode:     ModeStopped,
		state:    snapshot.State,
		revision: snapshot.Revision,
	}, nil
}

func validateLimits(limits Limits) error {
	if limits.OperationTimeout <= 0 || limits.VerificationTimeout <= 0 {
		return ErrInvalidLimits
	}
	return nil
}

// Start runs every Reconciler against the initial or restored Snapshot. A
// failure latches a Fault; success moves the Supervisor to Ready.
func (s *Supervisor[S, E, T]) Start(ctx context.Context, data T) Result[S, E] {
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
		base.Err = s.fault
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
	opCtx := s.beginOperationLocked(ctx, change, PhaseStart)
	snapshot := s.snapshotLocked()
	s.mu.Unlock()

	for _, reconcile := range s.machine.reconcile {
		outcome := s.invoke(opCtx, change, PhaseStart, func(callCtx context.Context) error {
			return reconcile(callCtx, snapshot, data)
		})
		if outcome.err != nil {
			cause := outcome.err
			if outcome.completed {
				cause = &ViolationError{Phase: PhaseStart, Reason: cause}
			}
			return s.faultResult(base, s.latch(change, PhaseStart, cause, false))
		}
	}
	if err := opCtx.Err(); err != nil {
		return s.faultResult(base, s.latch(change, PhaseStart, contextFailure(err), false))
	}
	if fault := s.finishReady(); fault != nil {
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
func (s *Supervisor[S, E, T]) Issue(ctx context.Context, event E, data T) Result[S, E] {
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
		base.Err = s.fault
		s.mu.Unlock()
		return base
	case ModeReady:
	}
	if s.attempt == math.MaxUint64 || s.revision == math.MaxUint64 {
		change := s.currentChangeLocked()
		change.Event = event
		s.mode = ModeExecuting
		s.active = activeOperation[S, E]{change: change, phase: PhaseSelection}
		s.mu.Unlock()
		return s.faultResult(base, s.latch(change, PhaseSelection, ErrCounterExhausted, false))
	}
	s.attempt++
	attempt := Attempt[S, E]{
		DefinitionID: s.machine.id,
		ID:           s.attempt,
		Revision:     s.revision,
		From:         s.state,
		Event:        event,
	}
	change := Change[S, E]{Attempt: attempt, To: s.state}
	base.Attempt = attempt.ID
	opCtx := s.beginOperationLocked(ctx, change, PhasePrecondition)
	s.mu.Unlock()

	for _, precondition := range s.machine.preconditions {
		outcome := s.invoke(opCtx, change, PhasePrecondition, func(callCtx context.Context) error {
			return precondition(callCtx, attempt, data)
		})
		if outcome.err != nil {
			cause := outcome.err
			if outcome.completed {
				cause = &ViolationError{Phase: PhasePrecondition, Reason: cause}
			}
			return s.faultResult(base, s.latch(change, PhasePrecondition, cause, false))
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
			outcome := s.invoke(opCtx, candidateChange, PhaseGuard, func(callCtx context.Context) error {
				return candidate.guard(callCtx, candidateChange, data)
			})
			if outcome.err != nil {
				if !outcome.completed {
					return s.faultResult(base, s.latch(candidateChange, PhaseGuard, outcome.err, false))
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
			return s.faultResult(base, s.latch(change, PhaseSelection, contextFailure(err), false))
		}
		if fault := s.finishReady(); fault != nil {
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

	if result, stopped := s.runChecks(opCtx, base, change, PhasePrecondition, selected.preconditions, data, false); stopped {
		return result
	}
	if result, stopped := s.runChecks(opCtx, base, change, PhaseInvariant, s.machine.invariants, data, false); stopped {
		return result
	}

	if selected.issue == nil {
		if result, stopped := s.runChecks(opCtx, base, change, PhasePostcondition, s.machine.postconditions, data, false); stopped {
			return result
		}
		return s.commit(opCtx, base, change, false, false)
	}

	outcome := s.invoke(opCtx, change, PhaseIssue, func(callCtx context.Context) error {
		return selected.issue(callCtx, change, data)
	})
	if outcome.err != nil {
		return s.faultResult(base, s.latch(change, PhaseIssue, outcome.err, true))
	}
	base.IssueCompleted = true
	base.Phase = PhaseIssue
	if err := opCtx.Err(); err != nil {
		return s.faultResult(base, s.latch(change, PhaseIssue, contextFailure(err), true))
	}
	if fault := s.awaitVerification(change, selected); fault != nil {
		return s.faultResult(base, fault)
	}
	return base
}

// Verify completes an issued Change using fresh application data. Verify,
// Invariants, and Postconditions must all succeed before logical commit.
func (s *Supervisor[S, E, T]) Verify(ctx context.Context, attempt uint64, data T) Result[S, E] {
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
		base.Err = s.fault
		s.mu.Unlock()
		return base
	case ModeAwaitingVerification:
	}
	if s.pending == nil {
		base.Err = ErrNoPending
		s.mu.Unlock()
		return base
	}
	if s.pending.change.ID != attempt {
		base.Attempt = attempt
		base.Err = ErrStaleAttempt
		s.mu.Unlock()
		return base
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
	opCtx := s.beginOperationLocked(ctx, change, PhaseVerify)
	s.mu.Unlock()

	outcome := s.invoke(opCtx, change, PhaseVerify, func(callCtx context.Context) error {
		return pending.transition.verify(callCtx, change, data)
	})
	if outcome.err != nil {
		cause := outcome.err
		if outcome.completed {
			cause = &ViolationError{Phase: PhaseVerify, Reason: cause}
		}
		return s.faultResult(base, s.latch(change, PhaseVerify, cause, true))
	}
	base.Verified = true
	if result, stopped := s.runChecks(opCtx, base, change, PhaseInvariant, s.machine.invariants, data, true); stopped {
		return result
	}
	if result, stopped := s.runChecks(opCtx, base, change, PhasePostcondition, s.machine.postconditions, data, true); stopped {
		return result
	}
	return s.commit(opCtx, base, change, true, true)
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
	uncertain := false
	if s.mode == ModeExecuting {
		change = s.active.change
		phase = s.active.phase
		uncertain = phase == PhaseIssue || s.pending != nil
	} else if s.mode == ModeAwaitingVerification && s.pending != nil {
		change = s.pending.change
		phase = PhaseVerify
		uncertain = true
	}
	fault, cancel := s.latchLocked(change, phase, reason, uncertain)
	copy := *fault
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return copy
}

// Recover runs every Reconciler against the last committed Snapshot. Recovery
// is refused while a timed-out callback still runs. A failed recovery retains
// the original first-cause Fault.
func (s *Supervisor[S, E, T]) Recover(ctx context.Context, data T) Result[S, E] {
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
	if s.callbacks != 0 {
		base.Faulted = true
		base.Uncertain = s.fault.Uncertain
		base.Err = ErrCallbackRunning
		s.mu.Unlock()
		return base
	}
	original := s.fault
	change := s.currentChangeLocked()
	opCtx := s.beginOperationLocked(ctx, change, PhaseRecover)
	snapshot := s.snapshotLocked()
	s.mu.Unlock()

	for _, reconcile := range s.machine.reconcile {
		outcome := s.invoke(opCtx, change, PhaseRecover, func(callCtx context.Context) error {
			return reconcile(callCtx, snapshot, data)
		})
		if outcome.err != nil {
			cause := outcome.err
			if outcome.completed {
				cause = &ViolationError{Phase: PhaseRecover, Reason: cause}
			}
			s.restoreFault(original)
			base.Phase = PhaseRecover
			base.Faulted = true
			base.Uncertain = original.Uncertain
			base.Err = errors.Join(original, cause)
			return base
		}
	}
	if err := opCtx.Err(); err != nil {
		s.restoreFault(original)
		base.Phase = PhaseRecover
		base.Faulted = true
		base.Uncertain = original.Uncertain
		base.Err = errors.Join(original, contextFailure(err))
		return base
	}

	s.mu.Lock()
	if s.mode != ModeExecuting || s.fault != original {
		fault := s.fault
		s.mu.Unlock()
		return s.faultResult(base, fault)
	}
	cancel := s.cancel
	s.cancel = nil
	s.active = activeOperation[S, E]{}
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
func (s *Supervisor[S, E, T]) Snapshot() Snapshot[S] {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

// Status returns one atomic execution-health snapshot.
func (s *Supervisor[S, E, T]) Status() Status[S, E] {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := Status[S, E]{
		Mode:            s.mode,
		Snapshot:        s.snapshotLocked(),
		Attempt:         s.attempt,
		CallbackRunning: s.callbacks != 0,
	}
	if s.pending != nil {
		pending := s.pending.change
		status.Pending = &pending
	}
	if s.fault != nil {
		fault := *s.fault
		status.Fault = &fault
	}
	return status
}

func (s *Supervisor[S, E, T]) runChecks(
	ctx context.Context,
	base Result[S, E],
	change Change[S, E],
	phase Phase,
	checks []Check[S, E, T],
	data T,
	uncertain bool,
) (Result[S, E], bool) {
	for _, check := range checks {
		outcome := s.invoke(ctx, change, phase, func(callCtx context.Context) error {
			return check(callCtx, change, data)
		})
		if outcome.err == nil {
			continue
		}
		cause := outcome.err
		if outcome.completed {
			cause = &ViolationError{Phase: phase, Reason: cause}
		}
		return s.faultResult(base, s.latch(change, phase, cause, uncertain)), true
	}
	return base, false
}

type callbackOutcome struct {
	err       error
	completed bool
}

func (s *Supervisor[S, E, T]) invoke(
	ctx context.Context,
	change Change[S, E],
	phase Phase,
	callback func(context.Context) error,
) callbackOutcome {
	if err := ctx.Err(); err != nil {
		return callbackOutcome{err: contextFailure(err)}
	}
	s.mu.Lock()
	if s.mode != ModeExecuting {
		fault := s.fault
		s.mu.Unlock()
		if fault != nil {
			return callbackOutcome{err: fault}
		}
		return callbackOutcome{err: ErrBusy}
	}
	s.active = activeOperation[S, E]{change: change, phase: phase}
	s.callbacks++
	s.mu.Unlock()

	done := make(chan callbackOutcome, 1)
	go func() {
		outcome := callbackOutcome{}
		returned := false
		defer func() {
			if !returned {
				if recovered := recover(); recovered != nil {
					outcome.err = &PanicError{Value: recovered}
				} else {
					outcome.err = ErrExecutionStopped
				}
			} else {
				outcome.completed = true
			}
			s.mu.Lock()
			s.callbacks--
			s.mu.Unlock()
			done <- outcome
		}()
		outcome.err = callback(ctx)
		returned = true
	}()

	select {
	case outcome := <-done:
		if err := ctx.Err(); err != nil {
			return callbackOutcome{err: contextFailure(err)}
		}
		return outcome
	case <-ctx.Done():
		return callbackOutcome{err: contextFailure(ctx.Err())}
	}
}

func contextFailure(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return &operationTimeoutError{cause: err}
	}
	return err
}

func (s *Supervisor[S, E, T]) awaitVerification(
	change Change[S, E],
	transition compiledTransition[S, E, T],
) *Fault[S, E] {
	s.mu.Lock()
	if s.mode != ModeExecuting {
		fault := s.fault
		s.mu.Unlock()
		return fault
	}
	cancel := s.cancel
	s.cancel = nil
	s.active = activeOperation[S, E]{}
	s.pending = &pendingChange[S, E, T]{change: change, transition: transition}
	s.mode = ModeAwaitingVerification
	s.timer = time.AfterFunc(s.limits.VerificationTimeout, func() {
		s.verificationExpired(change.ID)
	})
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

func (s *Supervisor[S, E, T]) verificationExpired(attempt uint64) {
	s.mu.Lock()
	if s.mode != ModeAwaitingVerification || s.pending == nil || s.pending.change.ID != attempt {
		s.mu.Unlock()
		return
	}
	change := s.pending.change
	fault, cancel := s.latchLocked(change, PhaseVerify, ErrVerificationTimeout, true)
	_ = fault
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *Supervisor[S, E, T]) commit(
	ctx context.Context,
	base Result[S, E],
	change Change[S, E],
	issueCompleted bool,
	verified bool,
) Result[S, E] {
	if err := ctx.Err(); err != nil {
		return s.faultResult(base, s.latch(change, PhaseCommit, contextFailure(err), issueCompleted))
	}
	s.mu.Lock()
	if s.mode != ModeExecuting {
		fault := s.fault
		s.mu.Unlock()
		return s.faultResult(base, fault)
	}
	if s.revision == math.MaxUint64 {
		fault, cancel := s.latchLocked(change, PhaseCommit, ErrCounterExhausted, issueCompleted)
		s.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return s.faultResult(base, fault)
	}
	s.state = change.To
	s.revision++
	s.pending = nil
	cancel := s.cancel
	s.cancel = nil
	s.active = activeOperation[S, E]{}
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
) context.Context {
	ctx, cancel := context.WithTimeout(parent, s.limits.OperationTimeout)
	s.mode = ModeExecuting
	s.active = activeOperation[S, E]{change: change, phase: phase}
	s.cancel = cancel
	return ctx
}

func (s *Supervisor[S, E, T]) finishReady() *Fault[S, E] {
	s.mu.Lock()
	if s.mode != ModeExecuting {
		fault := s.fault
		s.mu.Unlock()
		return fault
	}
	cancel := s.cancel
	s.cancel = nil
	s.active = activeOperation[S, E]{}
	s.mode = ModeReady
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

func (s *Supervisor[S, E, T]) restoreFault(fault *Fault[S, E]) {
	s.mu.Lock()
	cancel := s.cancel
	s.cancel = nil
	s.active = activeOperation[S, E]{}
	s.fault = fault
	s.mode = ModeFaulted
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *Supervisor[S, E, T]) latch(
	change Change[S, E],
	phase Phase,
	cause error,
	uncertain bool,
) *Fault[S, E] {
	s.mu.Lock()
	fault, cancel := s.latchLocked(change, phase, cause, uncertain)
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return fault
}

func (s *Supervisor[S, E, T]) latchLocked(
	change Change[S, E],
	phase Phase,
	cause error,
	uncertain bool,
) (*Fault[S, E], context.CancelFunc) {
	if s.fault == nil {
		s.fault = &Fault[S, E]{
			DefinitionID: s.machine.id,
			Attempt:      change.ID,
			Revision:     s.revision,
			State:        s.state,
			Event:        change.Event,
			TransitionID: change.TransitionID,
			Phase:        phase,
			Uncertain:    uncertain,
			Cause:        cause,
		}
	}
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	s.pending = nil
	cancel := s.cancel
	s.cancel = nil
	s.mode = ModeFaulted
	return s.fault, cancel
}

func (s *Supervisor[S, E, T]) snapshotLocked() Snapshot[S] {
	return Snapshot[S]{
		DefinitionID: s.machine.id,
		State:        s.state,
		Revision:     s.revision,
	}
}

func (s *Supervisor[S, E, T]) currentChangeLocked() Change[S, E] {
	return Change[S, E]{
		Attempt: Attempt[S, E]{
			DefinitionID: s.machine.id,
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
		Attempt:      s.attempt,
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

func resultFromChange[S, E comparable](change Change[S, E]) Result[S, E] {
	return Result[S, E]{
		DefinitionID: change.DefinitionID,
		Attempt:      change.ID,
		Revision:     change.Revision,
		From:         change.From,
		Event:        change.Event,
		TransitionID: change.TransitionID,
		To:           change.To,
		Selected:     true,
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
