package supervised

import (
	"errors"
	"fmt"
	"time"

	"github.com/open-ships/statemachine"
)

var (
	// ErrNotPermitted is statemachine.ErrNotPermitted.
	ErrNotPermitted = statemachine.ErrNotPermitted

	// ErrNilMachine reports construction from a nil Machine.
	ErrNilMachine = errors.New("supervised: nil machine")
	// ErrInvalidLimits reports zero or negative execution limits.
	ErrInvalidLimits = errors.New("supervised: limits must be positive and finite")
	// ErrNotStarted reports Issue before successful Start.
	ErrNotStarted = errors.New("supervised: supervisor not started")
	// ErrAlreadyStarted reports Start after successful startup.
	ErrAlreadyStarted = errors.New("supervised: supervisor already started")
	// ErrBusy reports overlap with a callback-bearing operation.
	ErrBusy = errors.New("supervised: operation already in flight")
	// ErrAwaitingVerification reports Issue while a Change awaits Verify.
	ErrAwaitingVerification = errors.New("supervised: awaiting verification")
	// ErrNoPending reports Verify without an issued Change.
	ErrNoPending = errors.New("supervised: no change awaits verification")
	// ErrStaleAttempt reports Verify with the wrong Attempt identifier.
	ErrStaleAttempt = errors.New("supervised: stale attempt")
	// ErrFaulted reports that a Fault is latched.
	ErrFaulted = errors.New("supervised: fault latched")
	// ErrNotFaulted reports Recover without a latched Fault.
	ErrNotFaulted = errors.New("supervised: no fault to recover")
	// ErrCallbackRunning reports recovery while a timed-out callback still runs.
	ErrCallbackRunning = errors.New("supervised: callback still running")
	// ErrOperationTimeout reports expiration of an operation's execution budget.
	ErrOperationTimeout = errors.New("supervised: operation timed out")
	// ErrVerificationTimeout reports an issued Change not verified in time.
	ErrVerificationTimeout = errors.New("supervised: verification timed out")
	// ErrExecutionStopped reports runtime.Goexit from a callback.
	ErrExecutionStopped = errors.New("supervised: callback stopped its goroutine")
	// ErrDefinitionMismatch reports restoration under a different definition ID.
	ErrDefinitionMismatch = errors.New("supervised: snapshot definition mismatch")
	// ErrUnknownState reports restoration at an undeclared state.
	ErrUnknownState = errors.New("supervised: snapshot state is undeclared")
	// ErrCounterExhausted reports an Attempt or Revision counter at MaxUint64.
	ErrCounterExhausted = errors.New("supervised: counter exhausted")
	// ErrTripped is used when Trip receives a nil reason.
	ErrTripped = errors.New("supervised: externally tripped")
	// ErrViolation identifies a mandatory check that returned an error.
	ErrViolation = errors.New("supervised: mandatory check failed")
	// ErrNilContext reports a nil context argument.
	ErrNilContext = errors.New("supervised: nil context")
	// ErrNilWriter reports a nil DOT destination.
	ErrNilWriter = errors.New("supervised: nil writer")
)

// Phase identifies the execution phase represented by a Result or Fault.
type Phase uint8

const (
	PhaseNone Phase = iota
	PhaseStart
	PhasePrecondition
	PhaseSelection
	PhaseGuard
	PhaseInvariant
	PhaseIssue
	PhaseVerify
	PhasePostcondition
	PhaseCommit
	PhaseRecover
	PhaseTrip
)

func (p Phase) String() string {
	switch p {
	case PhaseNone:
		return "none"
	case PhaseStart:
		return "start"
	case PhasePrecondition:
		return "precondition"
	case PhaseSelection:
		return "selection"
	case PhaseGuard:
		return "guard"
	case PhaseInvariant:
		return "invariant"
	case PhaseIssue:
		return "issue"
	case PhaseVerify:
		return "verify"
	case PhasePostcondition:
		return "postcondition"
	case PhaseCommit:
		return "commit"
	case PhaseRecover:
		return "recover"
	case PhaseTrip:
		return "trip"
	default:
		return fmt.Sprintf("Phase(%d)", uint8(p))
	}
}

// Mode identifies a Supervisor's execution mode.
type Mode uint8

const (
	ModeStopped Mode = iota
	ModeReady
	ModeExecuting
	ModeAwaitingVerification
	ModeFaulted
)

func (m Mode) String() string {
	switch m {
	case ModeStopped:
		return "stopped"
	case ModeReady:
		return "ready"
	case ModeExecuting:
		return "executing"
	case ModeAwaitingVerification:
		return "awaiting verification"
	case ModeFaulted:
		return "faulted"
	default:
		return fmt.Sprintf("Mode(%d)", uint8(m))
	}
}

// Limits bounds one callback-bearing operation and the interval between Issue
// and Verify. Both values must be positive.
type Limits struct {
	OperationTimeout    time.Duration
	VerificationTimeout time.Duration
}

// Snapshot is the durable logical state required to restore a Supervisor. It
// contains no claim about external physical state; Start must reconcile it.
type Snapshot[S comparable] struct {
	DefinitionID string
	State        S
	Revision     uint64
}

// Result describes one Start, Attempt, Verify, or Recover outcome. Err is nil
// on success. IssueCompleted false never proves that an external system did not
// receive a partial command.
type Result[S, E comparable] struct {
	DefinitionID   string
	Attempt        uint64
	Revision       uint64
	From           S
	Event          E
	TransitionID   string
	To             S
	Phase          Phase
	Selected       bool
	IssueCompleted bool
	Verified       bool
	Committed      bool
	Faulted        bool
	Uncertain      bool
	Err            error
}

// Fault is the immutable first cause latched by a Supervisor. Uncertain means
// external physical effects may have occurred or remain in progress.
type Fault[S, E comparable] struct {
	DefinitionID string
	Attempt      uint64
	Revision     uint64
	State        S
	Event        E
	TransitionID string
	Phase        Phase
	Uncertain    bool
	Cause        error
}

func (f *Fault[S, E]) Error() string {
	if f == nil {
		return "<nil>"
	}
	return fmt.Sprintf(
		"supervised: fault in %s phase at state %v on event %v: %v",
		f.Phase, f.State, f.Event, f.Cause,
	)
}

// Unwrap makes every Fault match ErrFaulted and its application cause.
func (f *Fault[S, E]) Unwrap() []error {
	if f == nil {
		return nil
	}
	return []error{ErrFaulted, f.Cause}
}

// PanicError reports a panic value contained inside a supervised callback.
type PanicError struct {
	Value any
}

func (e *PanicError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("supervised: callback panicked: %v", e.Value)
}

// ViolationError preserves the mandatory check's reason.
type ViolationError struct {
	Phase  Phase
	Reason error
}

func (e *ViolationError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("supervised: %s: %v", e.Phase, e.Reason)
}

func (e *ViolationError) Unwrap() []error {
	if e == nil {
		return nil
	}
	return []error{ErrViolation, e.Reason}
}

type operationTimeoutError struct {
	cause error
}

func (e *operationTimeoutError) Error() string {
	return fmt.Sprintf("%v: %v", ErrOperationTimeout, e.cause)
}

func (e *operationTimeoutError) Unwrap() []error {
	return []error{ErrOperationTimeout, e.cause}
}

type refusal[S, E comparable] struct {
	from   S
	event  E
	unwrap []error
}

func (r *refusal[S, E]) Error() string {
	return fmt.Sprintf("supervised: %v in state %v: %v", r.event, r.from, ErrNotPermitted)
}

func (r *refusal[S, E]) Unwrap() []error { return r.unwrap }
