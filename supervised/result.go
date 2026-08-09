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
	// ErrUnknownEvent reports an Issue value absent from the compiled Definition.
	ErrUnknownEvent = errors.New("supervised: event is undeclared")
	// ErrCounterExhausted reports an Attempt or Revision counter at MaxUint64.
	ErrCounterExhausted = errors.New("supervised: counter exhausted")
	// ErrInvalidSnapshot reports missing durable execution identity.
	ErrInvalidSnapshot = errors.New("supervised: snapshot has no execution identity")
	// ErrRestoredInDoubt reports a restored Snapshot whose prior execution may
	// have issued physical work or retained a Fault.
	ErrRestoredInDoubt = errors.New("supervised: restored execution requires reconciliation")
	// ErrJournalRequired reports safety configuration that requires a durable
	// Journal but did not provide one.
	ErrJournalRequired = errors.New("supervised: durable journal is required")
	// ErrJournal reports failure to durably prepare an external Issue.
	ErrJournal = errors.New("supervised: durable journal failed")
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

// PendingSnapshot is the durable identity of external work that may have
// reached a controller. It is evidence for reconciliation, never authority to
// replay or commit the Change.
type PendingSnapshot[S, E comparable] struct {
	Attempt              AttemptID
	Revision             uint64
	From                 S
	Event                E
	TransitionID         string
	To                   S
	IssuedAt             time.Time
	VerificationDeadline time.Time
}

// Snapshot is the durable execution state required to restore a Supervisor. It
// carries non-reusable execution identity and typed in-doubt Change identity;
// it contains no claim about external physical state. Start reconciles a clean
// Snapshot, while an in-doubt or faulted restore requires Recover.
type Snapshot[S, E comparable] struct {
	DefinitionID string
	ExecutionID  string
	State        S
	Revision     uint64
	Attempt      uint64
	Pending      *PendingSnapshot[S, E]
	InDoubt      bool
	Faulted      bool
	Uncertain    bool
	FaultPhase   Phase
	FaultCause   string
	RecordedAt   time.Time
}

// Result describes one Start, Attempt, Verify, or Recover outcome. The public
// operation also returns Err separately so tooling can detect discarded
// failures. IssueCompleted false never proves that an external system did not
// receive a partial command.
type Result[S, E comparable] struct {
	DefinitionID   string
	ExecutionID    string
	Operation      uint64
	AttemptKey     AttemptID
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
	StartedAt      time.Time
	CompletedAt    time.Time
	Err            error
}

// Fault is an outward snapshot of the immutable first cause retained privately
// by a Supervisor. Mutating this copy cannot change Supervisor state. Uncertain
// means external physical effects may have occurred or remain in progress.
type Fault[S, E comparable] struct {
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

func (f *Fault[S, E]) Error() string {
	if f == nil {
		return "<nil>"
	}
	cause := f.CauseText
	if cause == "" {
		cause = fmt.Sprint(f.Cause)
	}
	return fmt.Sprintf(
		"supervised: fault in %s phase at state %v on event %v: %s",
		f.Phase, f.State, f.Event, cause,
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
	Stack string
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
	message := fmt.Sprintf("supervised: %v in state %v: %v", r.event, r.from, ErrNotPermitted)
	if len(r.unwrap) > 1 {
		message += ": " + errors.Join(r.unwrap[1:]...).Error()
	}
	return message
}

func (r *refusal[S, E]) Unwrap() []error { return r.unwrap }
