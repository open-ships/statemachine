package supervised

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func expiryFixture(t *testing.T, options Options[testState, testEvent]) (*Supervisor[testState, testEvent, *testData], *fakeClock) {
	t.Helper()
	clock := newFakeClock()
	options.Clock = clock
	options.Limits = Limits{OperationTimeout: time.Minute, VerificationTimeout: time.Second}
	options.Unjournaled = true
	s, err := NewWithOptions(MustCompile(validDefinition()), options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Start(context.Background(), &testData{}); err != nil {
		t.Fatal(err)
	}
	return s, clock
}

func issueBeforeExpiry(t *testing.T, s *Supervisor[testState, testEvent, *testData], clock *fakeClock) Result[testState, testEvent] {
	t.Helper()
	issued, err := s.Issue(context.Background(), testStart, &testData{})
	if err != nil {
		t.Fatal(err)
	}
	clock.advance(time.Second) // Deliberately defer timer dispatch.
	return issued
}

func TestExpiredVerifyOwnsPublication(t *testing.T) {
	s, clock := expiryFixture(t, Options[testState, testEvent]{})
	issued := issueBeforeExpiry(t, s, clock)
	// Model preemption at Verify's decision/finalization seam, before any sink
	// starts. Running-sink checks alone cannot protect this interval.
	decision := s.verifyDecision(context.Background(), issued.AttemptKey, &testData{})
	if !errors.Is(decision.Err, ErrVerificationTimeout) {
		t.Fatal(decision.Err)
	}
	func() {
		defer s.finalize(RecordVerify, decision)
		if !s.Status().ReportingRunning {
			t.Error("expiry did not reserve publication")
		}
		if _, err := s.Recover(context.Background(), &testData{}); !errors.Is(err, ErrCallbackRunning) {
			t.Errorf("Recover before expiry publication = %v", err)
		}
		if _, err := s.Adjudicate(context.Background(), Decision[testState]{Outcome: AdjudicateAdopt}, &testData{}); !errors.Is(err, ErrCallbackRunning) {
			t.Errorf("Adjudicate before expiry publication = %v", err)
		}
	}()
	if s.Status().ReportingRunning {
		t.Fatal("expiry retained completed publication")
	}
	if _, err := s.Recover(context.Background(), &testData{}); err != nil {
		t.Fatal(err)
	}
}

func TestVerificationExpiryPreservesCommand(t *testing.T) {
	for _, source := range []string{"Verify", "timer"} {
		t.Run(source, func(t *testing.T) {
			s, clock := expiryFixture(t, Options[testState, testEvent]{})
			issued := issueBeforeExpiry(t, s, clock)
			kind := RecordVerificationExpired
			if source == "Verify" {
				kind = RecordVerify
				result, err := s.Verify(context.Background(), issued.AttemptKey, &testData{})
				if !errors.Is(err, ErrVerificationTimeout) || result.Change != issued.Change ||
					!result.Selected || !result.IssueCompleted || result.Committed || result.Verified {
					t.Fatalf("expired Verify = %+v, %v", result, err)
				}
			} else {
				clock.fireDue()
			}
			records := s.Records()
			record := records[len(records)-1]
			if record.Kind != kind || record.Change != issued.Change || !record.Selected || !record.Issued ||
				record.Committed || record.Verified || !record.Faulted || !record.Uncertain {
				t.Fatalf("expiry lost command evidence: %+v; original %+v", record, issued.Change)
			}
			if !record.OperationStartedAt.Equal(clock.Now()) || !record.Change.StartedAt.Before(record.OperationStartedAt) {
				t.Fatalf("expiry reused command start time: %+v", record)
			}
			if status := s.Status(); status.ReportingRunning || status.Fault.Change != issued.Change {
				t.Fatalf("expiry status = %+v", status)
			}
		})
	}
}

func TestTimerExpiryRetainsEnclosingIssueOperation(t *testing.T) {
	s, clock := expiryFixture(t, Options[testState, testEvent]{})
	// Hold the original public call at its finalization seam while the
	// verification timer publishes. Expiry may account for callbacks against
	// this Operation, but cannot mark the Issue call as returned.
	issued := s.issueDecision(context.Background(), testStart, &testData{})
	if issued.Err != nil {
		t.Fatal(issued.Err)
	}
	func() {
		defer s.finalize(RecordIssue, issued)
		clock.advance(time.Second)
		clock.fireDue()
		if status := s.Status(); !status.OperationRunning || status.ReportingRunning {
			t.Errorf("timer released enclosing Issue: %+v", status)
		}
		if _, err := s.Recover(context.Background(), &testData{}); !errors.Is(err, ErrCallbackRunning) {
			t.Errorf("Recover during enclosing Issue = %v", err)
		}
	}()
	if _, err := s.Recover(context.Background(), &testData{}); err != nil {
		t.Fatal(err)
	}
}

func TestExpiredVerifyDeliveryTimeoutBlocksRecovery(t *testing.T) {
	for _, sink := range []string{"Journal", "Recorder"} {
		t.Run(sink, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			var blocked atomic.Bool
			block := func() {
				if blocked.CompareAndSwap(false, true) {
					close(entered)
					<-release
				}
			}
			options := Options[testState, testEvent]{RecorderTimeout: 20 * time.Millisecond}
			if sink == "Journal" {
				options.Journal = func(_ context.Context, snapshot Snapshot[testState, testEvent]) error {
					if snapshot.Faulted {
						block()
					}
					return nil
				}
			} else {
				options.Recorder = func(_ context.Context, record Record[testState, testEvent]) error {
					if record.Kind == RecordVerify {
						block()
					}
					return nil
				}
			}
			s, clock := expiryFixture(t, options)
			issued := issueBeforeExpiry(t, s, clock)
			done := make(chan error, 1)
			go func() {
				_, err := s.Verify(context.Background(), issued.AttemptKey, &testData{})
				done <- err
			}()
			stableAwait(t, entered)
			if err := stableAwait(t, done); !errors.Is(err, ErrVerificationTimeout) {
				t.Fatal(err)
			}
			if !s.Status().CallbackRunning {
				t.Fatal("timed-out sink lost ownership")
			}
			if _, err := s.Recover(context.Background(), &testData{}); !errors.Is(err, ErrCallbackRunning) {
				t.Fatalf("Recover during detached %s = %v", sink, err)
			}
			once.Do(func() { close(release) })
			stableWaitIdle(t, s)
			if _, err := s.Recover(context.Background(), &testData{}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
