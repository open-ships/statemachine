package supervised

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	clock    *fakeClock
	deadline time.Time
	callback func()
	stopped  bool
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) AfterFunc(duration time.Duration, callback func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &fakeTimer{clock: c, deadline: c.now.Add(duration), callback: callback}
	c.timers = append(c.timers, timer)
	return timer
}

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := !t.stopped
	t.stopped = true
	return wasActive
}

func (c *fakeClock) advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

func (c *fakeClock) fireDue() {
	c.mu.Lock()
	var callbacks []func()
	for _, timer := range c.timers {
		if !timer.stopped && !c.now.Before(timer.deadline) {
			timer.stopped = true
			callbacks = append(callbacks, timer.callback)
		}
	}
	c.mu.Unlock()
	for _, callback := range callbacks {
		callback()
	}
}

func TestVerifyEnforcesStoredDeadlineAtAdmission(t *testing.T) {
	for _, test := range []struct {
		name      string
		advance   time.Duration
		wantFault bool
	}{
		{name: "before", advance: 9 * time.Second},
		{name: "equal", advance: 10 * time.Second, wantFault: true},
		{name: "after with delayed timer", advance: 11 * time.Second, wantFault: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			clock := newFakeClock()
			supervisor, err := NewWithClock(MustCompile(validDefinition()), Limits{
				OperationTimeout: time.Minute, VerificationTimeout: 10 * time.Second,
			}, clock)
			if err != nil {
				t.Fatal(err)
			}
			if result := supervisor.start(context.Background(), &testData{}); result.Err != nil {
				t.Fatal(result.Err)
			}
			issued := supervisor.issue(context.Background(), testStart, &testData{})
			clock.advance(test.advance) // deliberately do not dispatch the timer
			verified := supervisor.verify(context.Background(), issued.AttemptKey, &testData{})
			if test.wantFault {
				if !verified.Faulted || verified.Committed || !errors.Is(verified.Err, ErrVerificationTimeout) {
					t.Fatalf("Verify = %+v", verified)
				}
				clock.fireDue() // an already-delayed notification is now a no-op
				if status := supervisor.Status(); status.Mode != ModeFaulted || !errors.Is(status.Fault, ErrVerificationTimeout) {
					t.Fatalf("Status = %+v", status)
				}
				return
			}
			if verified.Err != nil || !verified.Committed {
				t.Fatalf("Verify = %+v", verified)
			}
		})
	}
}

func TestVerifyDeadlineAdmissionWinsRegardlessOfTimerMutexRace(t *testing.T) {
	for range 100 {
		clock := newFakeClock()
		supervisor, err := NewWithClock(MustCompile(validDefinition()), Limits{
			OperationTimeout: time.Minute, VerificationTimeout: 10 * time.Second,
		}, clock)
		if err != nil {
			t.Fatal(err)
		}
		_ = supervisor.start(context.Background(), &testData{})
		issued := supervisor.issue(context.Background(), testStart, &testData{})
		clock.advance(10 * time.Second)

		supervisor.mu.Lock()
		verifyDone := make(chan Result[testState, testEvent], 1)
		timerDone := make(chan struct{})
		go func() {
			verifyDone <- supervisor.verify(context.Background(), issued.AttemptKey, &testData{})
		}()
		go func() {
			clock.fireDue()
			close(timerDone)
		}()
		supervisor.mu.Unlock()

		result := <-verifyDone
		<-timerDone
		if !result.Faulted || result.Committed || !errors.Is(result.Err, ErrVerificationTimeout) {
			t.Fatalf("Verify = %+v", result)
		}
		if status := supervisor.Status(); status.Mode != ModeFaulted || !errors.Is(status.Fault, ErrVerificationTimeout) {
			t.Fatalf("Status = %+v", status)
		}
	}
}

func TestOperationTimeoutUsesInjectedClock(t *testing.T) {
	clock := newFakeClock()
	entered := make(chan struct{})
	definition := validDefinition()
	definition.Transitions[0].Issue = func(ctx context.Context, _ Change[testState, testEvent], _ *testData) error {
		close(entered)
		<-ctx.Done()
		return nil
	}
	supervisor, err := NewWithClock(MustCompile(definition), Limits{
		OperationTimeout: 10 * time.Second, VerificationTimeout: time.Minute,
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	_ = supervisor.start(context.Background(), &testData{})
	done := make(chan Result[testState, testEvent], 1)
	go func() { done <- supervisor.issue(context.Background(), testStart, &testData{}) }()
	<-entered
	clock.advance(10 * time.Second)
	clock.fireDue()
	result := <-done
	if !result.Faulted || !result.Uncertain || result.Committed ||
		!errors.Is(result.Err, ErrOperationTimeout) || !errors.Is(result.Err, context.DeadlineExceeded) {
		t.Fatalf("Issue = %+v", result)
	}
}

type expiringContext struct {
	context.Context
	done chan struct{}
	err  error
}

func (c *expiringContext) Done() <-chan struct{} { return c.done }
func (c *expiringContext) Err() error            { return c.err }

func TestInvokePreservesCompletedCallbackErrorAtDeadline(t *testing.T) {
	supervisor, _ := New(MustCompile(validDefinition()), limits())
	supervisor.mu.Lock()
	change := supervisor.currentChangeLocked()
	ctx := &expiringContext{Context: context.Background(), done: make(chan struct{})}
	op, _ := supervisor.beginOperationLocked(ctx, change, PhaseInvariant)
	supervisor.mu.Unlock()
	defer supervisor.operationReturned(op)

	sentinel := errors.New("mandatory check failed")
	outcome := supervisor.invoke(op, ctx, change, PhaseInvariant, func(context.Context) error {
		ctx.err = context.DeadlineExceeded
		close(ctx.done)
		return sentinel
	})
	if !outcome.completed || !errors.Is(outcome.err, sentinel) || !errors.Is(outcome.err, context.DeadlineExceeded) {
		t.Fatalf("outcome = %+v", outcome)
	}
}

func TestPostVerifyMandatoryFailuresLatchWithoutCommit(t *testing.T) {
	sentinel := errors.New("post-verify check failed")
	for _, phase := range []Phase{PhaseInvariant, PhasePostcondition} {
		t.Run(phase.String(), func(t *testing.T) {
			definition := validDefinition()
			if phase == PhaseInvariant {
				calls := 0
				definition.Invariants = []Check[testState, testEvent, *testData]{func(context.Context, Change[testState, testEvent], *testData) error {
					calls++
					if calls == 2 {
						return sentinel
					}
					return nil
				}}
			} else {
				definition.Postconditions = []Check[testState, testEvent, *testData]{func(context.Context, Change[testState, testEvent], *testData) error {
					return sentinel
				}}
			}
			supervisor, _ := New(MustCompile(definition), limits())
			_ = supervisor.start(context.Background(), &testData{})
			issued := supervisor.issue(context.Background(), testStart, &testData{})
			result := supervisor.verify(context.Background(), issued.AttemptKey, &testData{})
			if !result.IssueCompleted || !result.Verified || result.Committed || !result.Faulted ||
				!result.Uncertain || result.Phase != phase || !errors.Is(result.Err, ErrViolation) || !errors.Is(result.Err, sentinel) {
				t.Fatalf("Verify = %+v", result)
			}
			status := supervisor.Status()
			if status.Mode != ModeFaulted || status.Pending != nil || status.Snapshot.State != testIdle || status.Snapshot.Revision != 0 {
				t.Fatalf("Status = %+v", status)
			}
		})
	}
}
