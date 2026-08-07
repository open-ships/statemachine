package supervised

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
)

func TestConstructorAndLimitErrors(t *testing.T) {
	valid := MustCompile(validDefinition())
	if _, err := New[testState, testEvent, *testData](nil, limits()); !errors.Is(err, ErrNilMachine) {
		t.Fatalf("New nil = %v", err)
	}
	if _, err := New(valid, Limits{}); !errors.Is(err, ErrInvalidLimits) {
		t.Fatalf("New limits = %v", err)
	}
	if _, err := Restore[testState, testEvent, *testData](nil, Snapshot[testState]{}, limits()); !errors.Is(err, ErrNilMachine) {
		t.Fatalf("Restore nil = %v", err)
	}
	if _, err := Restore(valid, Snapshot[testState]{DefinitionID: valid.ID(), State: testIdle}, Limits{}); !errors.Is(err, ErrInvalidLimits) {
		t.Fatalf("Restore limits = %v", err)
	}
	if _, err := Restore(valid, Snapshot[testState]{DefinitionID: valid.ID(), State: testIdle, Revision: math.MaxUint64}, limits()); !errors.Is(err, ErrCounterExhausted) {
		t.Fatalf("Restore counter = %v", err)
	}
}

func TestStartFailureNilContextAndStatusErrors(t *testing.T) {
	definition := validDefinition()
	definition.Reconcile = []Reconciler[testState, *testData]{
		func(context.Context, Snapshot[testState], *testData) error { return errReconcile },
	}
	supervisor, _ := New(MustCompile(definition), limits())
	//nolint:staticcheck // This test verifies the defensive nil-context result.
	if result := supervisor.Start(nil, &testData{}); !errors.Is(result.Err, ErrNilContext) {
		t.Fatalf("nil Start = %+v", result)
	}
	failed := supervisor.Start(context.Background(), &testData{})
	if !failed.Faulted || failed.Uncertain || !errors.Is(failed.Err, ErrViolation) || !errors.Is(failed.Err, errReconcile) {
		t.Fatalf("failed Start = %+v", failed)
	}
	if again := supervisor.Start(context.Background(), &testData{}); !errors.Is(again.Err, errReconcile) {
		t.Fatalf("faulted Start = %+v", again)
	}
	//nolint:staticcheck // This test verifies the defensive nil-context result.
	if nilIssue := supervisor.Issue(nil, testStart, &testData{}); !nilIssue.Faulted || !errors.Is(nilIssue.Err, ErrNilContext) {
		t.Fatalf("nil Issue = %+v", nilIssue)
	}
	//nolint:staticcheck // This test verifies the defensive nil-context result.
	if nilVerify := supervisor.Verify(nil, 1, &testData{}); !nilVerify.Faulted || !errors.Is(nilVerify.Err, ErrNilContext) {
		t.Fatalf("nil Verify = %+v", nilVerify)
	}
	//nolint:staticcheck // This test verifies the defensive nil-context result.
	if nilRecover := supervisor.Recover(nil, &testData{}); !nilRecover.Faulted || !errors.Is(nilRecover.Err, ErrNilContext) {
		t.Fatalf("nil Recover = %+v", nilRecover)
	}
}

func TestStartOverlapAndAlreadyStarted(t *testing.T) {
	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	definition := validDefinition()
	definition.Reconcile = []Reconciler[testState, *testData]{
		func(context.Context, Snapshot[testState], *testData) error {
			entered <- struct{}{}
			<-block
			return nil
		},
	}
	supervisor, _ := New(MustCompile(definition), limits())
	resultCh := make(chan Result[testState, testEvent], 1)
	go func() { resultCh <- supervisor.Start(context.Background(), &testData{}) }()
	<-entered
	if overlap := supervisor.Start(context.Background(), &testData{}); !errors.Is(overlap.Err, ErrBusy) {
		t.Fatalf("overlap Start = %+v", overlap)
	}
	close(block)
	if result := <-resultCh; result.Err != nil {
		t.Fatalf("Start = %+v", result)
	}
	if again := supervisor.Start(context.Background(), &testData{}); !errors.Is(again.Err, ErrAlreadyStarted) {
		t.Fatalf("second Start = %+v", again)
	}
	if recovery := supervisor.Recover(context.Background(), &testData{}); !errors.Is(recovery.Err, ErrNotFaulted) {
		t.Fatalf("Recover while ready = %+v", recovery)
	}
}

func TestGlobalPreconditionFailsBeforeGuard(t *testing.T) {
	guardCalls := 0
	definition := validDefinition()
	definition.Preconditions = []Precondition[testState, testEvent, *testData]{
		func(context.Context, Attempt[testState, testEvent], *testData) error { return errPrecondition },
	}
	definition.Transitions[0].Guard = func(context.Context, Change[testState, testEvent], *testData) error {
		guardCalls++
		return nil
	}
	supervisor, _ := New(MustCompile(definition), limits())
	_ = supervisor.Start(context.Background(), &testData{})
	result := supervisor.Issue(context.Background(), testStart, &testData{})
	if !result.Faulted || result.Selected || guardCalls != 0 || !errors.Is(result.Err, errPrecondition) {
		t.Fatalf("Issue = %+v, guard calls %d", result, guardCalls)
	}
}

func TestInvariantAndPostconditionFailuresLatchAtTheirPhases(t *testing.T) {
	tests := []struct {
		name      string
		phase     Phase
		configure func(*Definition[testState, testEvent, *testData])
	}{
		{
			name:  "invariant",
			phase: PhaseInvariant,
			configure: func(definition *Definition[testState, testEvent, *testData]) {
				definition.Invariants = []Check[testState, testEvent, *testData]{
					func(context.Context, Change[testState, testEvent], *testData) error { return errPrecondition },
				}
			},
		},
		{
			name:  "logical postcondition",
			phase: PhasePostcondition,
			configure: func(definition *Definition[testState, testEvent, *testData]) {
				definition.Transitions[0].Issue = nil
				definition.Transitions[0].Verify = nil
				definition.Postconditions = []Check[testState, testEvent, *testData]{
					func(context.Context, Change[testState, testEvent], *testData) error { return errPrecondition },
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := validDefinition()
			test.configure(&definition)
			supervisor, _ := New(MustCompile(definition), limits())
			_ = supervisor.Start(context.Background(), &testData{})
			result := supervisor.Issue(context.Background(), testStart, &testData{})
			if !result.Faulted || result.Phase != test.phase || result.Uncertain || !errors.Is(result.Err, errPrecondition) {
				t.Fatalf("Issue = %+v", result)
			}
		})
	}
}

func TestIssueAndVerifyModeErrors(t *testing.T) {
	machine := MustCompile(validDefinition())
	stopped, _ := New(machine, limits())
	if result := stopped.Verify(context.Background(), 1, &testData{}); !errors.Is(result.Err, ErrNotStarted) {
		t.Fatalf("stopped Verify = %+v", result)
	}

	supervisor, _ := New(machine, limits())
	_ = supervisor.Start(context.Background(), &testData{})
	issued := supervisor.Issue(context.Background(), testStart, &testData{})
	if result := supervisor.Issue(context.Background(), testStart, &testData{}); !errors.Is(result.Err, ErrAwaitingVerification) {
		t.Fatalf("Issue while awaiting = %+v", result)
	}
	fault := supervisor.Trip(nil)
	if !errors.Is(&fault, ErrTripped) || fault.Phase != PhaseVerify || !fault.Uncertain {
		t.Fatalf("Trip nil = %+v", fault)
	}
	if result := supervisor.Verify(context.Background(), issued.Attempt, &testData{}); !errors.Is(result.Err, ErrTripped) {
		t.Fatalf("faulted Verify = %+v", result)
	}
}

func TestIssueOverlapIsFailFast(t *testing.T) {
	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	definition := validDefinition()
	definition.Preconditions = []Precondition[testState, testEvent, *testData]{
		func(context.Context, Attempt[testState, testEvent], *testData) error {
			entered <- struct{}{}
			<-block
			return nil
		},
	}
	supervisor, _ := New(MustCompile(definition), limits())
	_ = supervisor.Start(context.Background(), &testData{})
	resultCh := make(chan Result[testState, testEvent], 1)
	go func() { resultCh <- supervisor.Issue(context.Background(), testStart, &testData{}) }()
	<-entered
	if overlap := supervisor.Issue(context.Background(), testStart, &testData{}); !errors.Is(overlap.Err, ErrBusy) {
		t.Fatalf("overlap Issue = %+v", overlap)
	}
	if verify := supervisor.Verify(context.Background(), 1, &testData{}); !errors.Is(verify.Err, ErrBusy) {
		t.Fatalf("Verify during Issue = %+v", verify)
	}
	close(block)
	issued := <-resultCh
	if issued.Err != nil || !issued.IssueCompleted {
		t.Fatalf("Issue = %+v", issued)
	}
	_ = supervisor.Trip(errTrip)
}

func TestCancelledOperationLatchesAndCounterExhaustionFaults(t *testing.T) {
	machine := MustCompile(validDefinition())
	supervisor, _ := New(machine, limits())
	_ = supervisor.Start(context.Background(), &testData{})
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	result := supervisor.Issue(cancelled, testStart, &testData{})
	if !result.Faulted || !errors.Is(result.Err, context.Canceled) {
		t.Fatalf("cancelled Issue = %+v", result)
	}

	supervisor, _ = New(machine, limits())
	_ = supervisor.Start(context.Background(), &testData{})
	supervisor.mu.Lock()
	supervisor.attempt = math.MaxUint64
	supervisor.mu.Unlock()
	result = supervisor.Issue(context.Background(), testStart, &testData{})
	if !result.Faulted || !errors.Is(result.Err, ErrCounterExhausted) {
		t.Fatalf("exhausted Issue = %+v", result)
	}
}

func TestTripInsideIssueAndPostconditionPreventsCommit(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*Definition[testState, testEvent, *testData], **Supervisor[testState, testEvent, *testData])
	}{
		{
			name: "issue",
			configure: func(definition *Definition[testState, testEvent, *testData], supervisor **Supervisor[testState, testEvent, *testData]) {
				definition.Transitions[0].Issue = func(context.Context, Change[testState, testEvent], *testData) error {
					(*supervisor).Trip(errTrip)
					return nil
				}
			},
		},
		{
			name: "postcondition",
			configure: func(definition *Definition[testState, testEvent, *testData], supervisor **Supervisor[testState, testEvent, *testData]) {
				definition.Transitions[0].Issue = nil
				definition.Transitions[0].Verify = nil
				definition.Postconditions = []Check[testState, testEvent, *testData]{
					func(context.Context, Change[testState, testEvent], *testData) error {
						(*supervisor).Trip(errTrip)
						return nil
					},
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := validDefinition()
			var supervisor *Supervisor[testState, testEvent, *testData]
			test.configure(&definition, &supervisor)
			supervisor, _ = New(MustCompile(definition), limits())
			_ = supervisor.Start(context.Background(), &testData{})
			result := supervisor.Issue(context.Background(), testStart, &testData{})
			if !result.Faulted || result.Committed || !errors.Is(result.Err, errTrip) {
				t.Fatalf("Issue = %+v", result)
			}
		})
	}
}

func TestCommitCounterDefenceAndExpiredTimerNoop(t *testing.T) {
	machine := MustCompile(validDefinition())
	supervisor, _ := New(machine, limits())
	_ = supervisor.Start(context.Background(), &testData{})
	issued := supervisor.Issue(context.Background(), testStart, &testData{})
	supervisor.mu.Lock()
	supervisor.revision = math.MaxUint64
	supervisor.mu.Unlock()
	result := supervisor.Verify(context.Background(), issued.Attempt, &testData{})
	if !result.Faulted || result.Committed || !errors.Is(result.Err, ErrCounterExhausted) {
		t.Fatalf("Verify = %+v", result)
	}
	supervisor.verificationExpired(issued.Attempt)

	defensive := supervisor.faultResult(Result[testState, testEvent]{}, nil)
	if !defensive.Faulted || !errors.Is(defensive.Err, ErrFaulted) {
		t.Fatalf("defensive fault result = %+v", defensive)
	}
}

func TestStringersAndErrorTypes(t *testing.T) {
	phases := []Phase{
		PhaseNone, PhaseStart, PhasePrecondition, PhaseSelection, PhaseGuard, PhaseInvariant,
		PhaseIssue, PhaseVerify, PhasePostcondition, PhaseCommit, PhaseRecover, PhaseTrip,
	}
	for _, phase := range phases {
		if phase.String() == "" {
			t.Fatalf("empty Phase string for %d", phase)
		}
	}
	if got := Phase(255).String(); got != "Phase(255)" {
		t.Fatalf("unknown Phase = %q", got)
	}
	for _, mode := range []Mode{ModeStopped, ModeReady, ModeExecuting, ModeAwaitingVerification, ModeFaulted} {
		if mode.String() == "" {
			t.Fatalf("empty Mode string for %d", mode)
		}
	}
	if got := Mode(255).String(); got != "Mode(255)" {
		t.Fatalf("unknown Mode = %q", got)
	}

	var nilFault *Fault[testState, testEvent]
	if nilFault.Error() != "<nil>" || nilFault.Unwrap() != nil {
		t.Fatalf("nil Fault = %q, %v", nilFault.Error(), nilFault.Unwrap())
	}
	fault := &Fault[testState, testEvent]{State: testIdle, Event: testStart, Phase: PhaseIssue, Cause: errIssue}
	if !strings.Contains(fault.Error(), "issue") || !errors.Is(fault, ErrFaulted) || !errors.Is(fault, errIssue) {
		t.Fatalf("Fault = %q", fault.Error())
	}
	var nilPanic *PanicError
	if nilPanic.Error() != "<nil>" {
		t.Fatalf("nil PanicError = %q", nilPanic.Error())
	}
	if got := (&PanicError{Value: "boom"}).Error(); !strings.Contains(got, "boom") {
		t.Fatalf("PanicError = %q", got)
	}
	var nilViolation *ViolationError
	if nilViolation.Error() != "<nil>" || nilViolation.Unwrap() != nil {
		t.Fatalf("nil ViolationError = %q, %v", nilViolation.Error(), nilViolation.Unwrap())
	}
	violation := &ViolationError{Phase: PhaseInvariant, Reason: errPrecondition}
	if !errors.Is(violation, ErrViolation) || !errors.Is(violation, errPrecondition) || !strings.Contains(violation.Error(), "invariant") {
		t.Fatalf("ViolationError = %q", violation.Error())
	}
	timeout := &operationTimeoutError{cause: context.DeadlineExceeded}
	if !errors.Is(timeout, ErrOperationTimeout) || !errors.Is(timeout, context.DeadlineExceeded) || timeout.Error() == "" {
		t.Fatalf("timeout = %q", timeout.Error())
	}
	refusal := &refusal[testState, testEvent]{from: testIdle, event: testStart, unwrap: []error{ErrNotPermitted}}
	if !errors.Is(refusal, ErrNotPermitted) || refusal.Error() == "" {
		t.Fatalf("refusal = %q", refusal.Error())
	}
}
