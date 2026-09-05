package supervised

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type diagnosticError struct{ text func() string }

func (e *diagnosticError) Error() string { return e.text() }

type hostileLabel string

func (s hostileLabel) String() string { panic("state/event String method called") }

func TestSafeErrorTextPreservesIdentityWithoutApplicationMethods(t *testing.T) {
	calls := 0
	original := &diagnosticError{text: func() string { calls++; return "custom diagnostic" }}
	frozen := captureErrorText(original)
	var extracted *diagnosticError
	if !errors.Is(frozen, original) || !errors.As(frozen, &extracted) || extracted != original || frozen.Error() != "custom diagnostic" || calls != 1 {
		t.Fatalf("captured diagnostic/identity = %q/%d", frozen.Error(), calls)
	}
	original.text = func() string { panic("cached error rendered twice") }
	if got := safeErrorText(errors.Join(errors.New("first"), frozen, fmt.Errorf("wrapped: %w", errors.New("last")))); got != "first\ncustom diagnostic\nwrapped: last" {
		t.Fatal(got)
	}
	fault := &Fault[hostileLabel, hostileLabel]{State: "idle", Event: "go", Phase: PhaseTrip, Cause: original}
	if got := fault.Error(); !strings.Contains(got, "state idle on event go") || !strings.Contains(got, "diagnosticError") {
		t.Fatal(got)
	}
	if got := (&PanicError{Value: hostileLabel("panic value")}).Error(); got != "supervised: callback panicked: panic value" {
		t.Fatal(got)
	}
	joined := errors.Join(errors.New("cycle"))
	joined.(interface{ Unwrap() []error }).Unwrap()[0] = joined
	if text := safeErrorText(joined); !strings.Contains(text, "error detail limit") {
		t.Fatal(text)
	}
	if text := captureErrorText(joined).Error(); !strings.Contains(text, "error detail limit") {
		t.Fatal(text)
	}
}

func TestCallbackErrorFormattingCanInspectSupervisor(t *testing.T) {
	var supervisor *Supervisor[testState, testEvent, *testData]
	var calls atomic.Int32
	original := &diagnosticError{text: func() string {
		calls.Add(1)
		if supervisor.Status().Mode != ModeExecuting {
			return "unexpected mode"
		}
		return "diagnostic read status"
	}}
	definition := validDefinition()
	definition.Preconditions = []Precondition[testState, testEvent, *testData]{func(context.Context, Attempt[testState, testEvent], *testData) error { return original }}
	var err error
	supervisor, err = newUnjournaled(MustCompile(definition), limits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = supervisor.Start(context.Background(), &testData{}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := supervisor.Issue(context.Background(), testStart, &testData{}); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, original) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Error() reentry blocked fault latching")
	}
	if calls.Load() != 1 || !strings.Contains(supervisor.Status().Fault.CauseText, "diagnostic read status") {
		t.Fatalf("calls=%d; fault=%v", calls.Load(), supervisor.Status().Fault)
	}
	records := supervisor.Records()
	if !strings.Contains(records[len(records)-1].CauseText, "diagnostic read status") {
		t.Fatal(records[len(records)-1])
	}
}

func TestBlockingErrorFormattingStaysInsideOperationOwnership(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	original := &diagnosticError{text: func() string { close(entered); <-release; return "late diagnostic" }}
	definition := validDefinition()
	definition.Preconditions = []Precondition[testState, testEvent, *testData]{func(context.Context, Attempt[testState, testEvent], *testData) error { return original }}
	bound := limits()
	bound.OperationTimeout = 30 * time.Millisecond
	supervisor, err := newUnjournaled(MustCompile(definition), bound)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = supervisor.Start(context.Background(), &testData{}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := supervisor.Issue(context.Background(), testStart, &testData{}); done <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("formatter did not start")
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrOperationTimeout) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocking formatter escaped operation timeout")
	}
	status := supervisor.Status()
	if !status.OperationRunning || !status.CallbackRunning || status.Mode != ModeFaulted {
		t.Fatalf("formatter ownership = %+v", status)
	}
	if _, err := supervisor.Recover(context.Background(), &testData{}); !errors.Is(err, ErrCallbackRunning) {
		t.Fatalf("Recover during formatter: %v", err)
	}
	close(release)
	waitForDiagnosticIdle(t, supervisor)
	if _, err := supervisor.Recover(context.Background(), &testData{}); err != nil {
		t.Fatal(err)
	}
}

func TestTripUsesSafeFallbackForUncapturedCustomReason(t *testing.T) {
	var calls atomic.Int32
	original := &diagnosticError{text: func() string { calls.Add(1); panic("Trip formatted arbitrary error") }}
	supervisor, err := newUnjournaled(MustCompile(validDefinition()), limits())
	if err != nil {
		t.Fatal(err)
	}
	fault := supervisor.Trip(original)
	if !errors.Is(&fault, original) || calls.Load() != 0 || !strings.Contains(fault.CauseText, "diagnosticError") {
		t.Fatalf("Trip = %v; calls=%d", &fault, calls.Load())
	}
	if supervisor.Status().Mode != ModeFaulted {
		t.Fatal("Trip did not latch")
	}
}

func TestFormattingPanicAndGoexitAreContained(t *testing.T) {
	for _, stop := range []bool{false, true} {
		t.Run(fmt.Sprint(stop), func(t *testing.T) {
			original := &diagnosticError{text: func() string {
				if stop {
					runtime.Goexit()
				}
				panic("diagnostic panic")
			}}
			definition := validDefinition()
			definition.Preconditions = []Precondition[testState, testEvent, *testData]{func(context.Context, Attempt[testState, testEvent], *testData) error { return original }}
			supervisor, err := newUnjournaled(MustCompile(definition), limits())
			if err != nil {
				t.Fatal(err)
			}
			if _, err = supervisor.Start(context.Background(), &testData{}); err != nil {
				t.Fatal(err)
			}
			_, err = supervisor.Issue(context.Background(), testStart, &testData{})
			if stop && !errors.Is(err, ErrExecutionStopped) || !stop && !errors.Is(err, original) {
				t.Fatalf("formatting failure: %v", err)
			}
			waitForDiagnosticIdle(t, supervisor)
		})
	}
}

func TestAdapterErrorFormattingRetainsDeliveryLease(t *testing.T) {
	for _, journal := range []bool{false, true} {
		t.Run(fmt.Sprint(journal), func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			original := &diagnosticError{text: func() string { calls.Add(1); close(entered); <-release; return "late adapter diagnostic" }}
			definition := validDefinition()
			definition.Transitions[0].Issue = nil
			definition.Transitions[0].Verify = nil
			options := Options[testState, testEvent]{Limits: limits(), RecorderTimeout: 30 * time.Millisecond, Unjournaled: true}
			if journal {
				options.Journal = func(_ context.Context, snapshot Snapshot[testState, testEvent]) error {
					if snapshot.Attempt > 0 {
						return original
					}
					return nil
				}
			} else {
				options.Recorder = func(_ context.Context, record Record[testState, testEvent]) error {
					if record.Kind == RecordIssue {
						return original
					}
					return nil
				}
			}
			supervisor, err := NewWithOptions(MustCompile(definition), options)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = supervisor.Start(context.Background(), &testData{}); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { _, err := supervisor.Issue(context.Background(), testStart, &testData{}); done <- err }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("adapter formatter did not start")
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("adapter formatter escaped delivery timeout")
			}
			status := supervisor.Status()
			if !status.OperationRunning || !status.CallbackRunning || journal && !status.JournalRunning || !journal && !status.RecorderRunning {
				t.Fatalf("adapter formatter ownership = %+v", status)
			}
			if calls.Load() != 1 {
				t.Fatalf("formatting calls = %d", calls.Load())
			}
			close(release)
			waitForDiagnosticIdle(t, supervisor)
		})
	}
}

func waitForDiagnosticIdle(t *testing.T, supervisor *Supervisor[testState, testEvent, *testData]) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		status := supervisor.Status()
		if !status.OperationRunning && !status.CallbackRunning {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("diagnostic operation did not drain: %+v", status)
		}
		runtime.Gosched()
	}
}

func TestUnknownOverrideStateDoesNotCallItsStringMethod(t *testing.T) {
	pass := func(context.Context, Change[hostileLabel, int], struct{}) error { return nil }
	definition := Definition[hostileLabel, int, struct{}]{
		ID: "hostile-label/v1", Initial: hostileLabel("idle"),
		States: []State[hostileLabel, int]{{Name: "idle", Terminal: true}}, Events: []int{1},
		Preconditions: []Precondition[hostileLabel, int, struct{}]{func(context.Context, Attempt[hostileLabel, int], struct{}) error { return nil }},
		Invariants:    []Check[hostileLabel, int, struct{}]{pass}, Postconditions: []Check[hostileLabel, int, struct{}]{pass},
		Reconcile: []Reconciler[hostileLabel, int, struct{}]{func(context.Context, Snapshot[hostileLabel, int], struct{}) error { return nil }},
	}
	supervisor, err := New(MustCompile(definition), limits())
	if err != nil {
		t.Fatal(err)
	}
	supervisor.Trip(nil)
	if _, err := supervisor.Adjudicate(context.Background(), Decision[hostileLabel]{Outcome: AdjudicateOverride, State: "unknown"}, struct{}{}); !errors.Is(err, ErrInvalidDecision) {
		t.Fatalf("unknown override: %v", err)
	}
	if supervisor.Status().Mode != ModeFaulted {
		t.Fatal("invalid override changed faulted state")
	}
}

// Exported error types can be embedded by application errors, promoting their
// internal safe renderer even when the embedded pointer is nil.
type embeddedDiagnosticFault struct{ *Fault[testState, testEvent] }

func (embeddedDiagnosticFault) Error() string { panic("application Error invoked") }
func TestTripHandlesNilEmbeddedLibraryError(t *testing.T) {
	supervisor, err := newUnjournaled(MustCompile(validDefinition()), limits())
	if err != nil {
		t.Fatal(err)
	}
	fault := supervisor.Trip(embeddedDiagnosticFault{})
	if fault.CauseText != "<nil>" || supervisor.Status().Mode != ModeFaulted {
		t.Fatalf("nil embedded diagnostic: %+v", fault)
	}
}

type nestedDiagnosticFault struct{ *embeddedDiagnosticFault }

func (nestedDiagnosticFault) Error() string { panic("application Error invoked") }
func TestTripHandlesNilPromotedErrorWrapper(t *testing.T) {
	supervisor, err := newUnjournaled(MustCompile(validDefinition()), limits())
	if err != nil {
		t.Fatal(err)
	}
	fault := supervisor.Trip(nestedDiagnosticFault{})
	if !strings.Contains(fault.CauseText, "nestedDiagnosticFault") || supervisor.Status().Mode != ModeFaulted {
		t.Fatalf("nil promoted diagnostic: %+v", fault)
	}
}
