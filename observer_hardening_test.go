package statemachine_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/open-ships/statemachine"
)

type obsState string
type obsEvent string

const (
	obsIdle    obsState = "idle"
	obsRunning obsState = "running"
	obsGo      obsEvent = "go"
)

func observerTable() *statemachine.Machine[obsState, obsEvent, int] {
	return statemachine.MustCompile([]statemachine.Transition[obsState, obsEvent, int]{
		{From: obsIdle, Event: obsGo, To: obsRunning},
	})
}

func TestTimeoutObserverBoundsBlockedDelivery(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	blocked := func(context.Context, statemachine.Observation[obsState, obsEvent], int) {
		<-release
	}
	instance := statemachine.NewInstanceWithObservers(observerTable(), obsIdle,
		statemachine.TimeoutObserver(blocked, 20*time.Millisecond))

	state, err := instance.Fire(context.Background(), obsGo, 0)
	if state != obsRunning || instance.State() != obsRunning {
		t.Fatalf("committed state = %v/%v, want running", state, instance.State())
	}
	if !errors.Is(err, statemachine.ErrObserverFailed) || !errors.Is(err, statemachine.ErrObserverTimeout) {
		t.Fatalf("Fire error = %v", err)
	}
}

func TestTimeoutObserverPassesPromptDeliveryAndPreservesPanics(t *testing.T) {
	calls := 0
	prompt := func(context.Context, statemachine.Observation[obsState, obsEvent], int) { calls++ }
	instance := statemachine.NewInstanceWithObservers(observerTable(), obsIdle,
		statemachine.TimeoutObserver(prompt, time.Second))
	if state, err := instance.Fire(context.Background(), obsGo, 0); err != nil || state != obsRunning || calls != 2 {
		t.Fatalf("prompt delivery = %v, %v, calls %d", state, err, calls)
	}

	panicking := func(context.Context, statemachine.Observation[obsState, obsEvent], int) {
		panic("observer exploded")
	}
	instance = statemachine.NewInstanceWithObservers(observerTable(), obsIdle,
		statemachine.TimeoutObserver(panicking, time.Second))
	state, err := instance.Fire(context.Background(), obsGo, 0)
	if state != obsRunning || !errors.Is(err, statemachine.ErrObserverFailed) {
		t.Fatalf("panicking delivery = %v, %v", state, err)
	}
	var observerError *statemachine.ObserverError
	if !errors.As(err, &observerError) || observerError.Stack == "" {
		t.Fatalf("panic evidence = %+v", err)
	}
}

func TestTimeoutObserverConstructionContract(t *testing.T) {
	if statemachine.TimeoutObserver[obsState, obsEvent, int](nil, time.Second) != nil {
		t.Fatal("nil observer did not collapse to nil")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("non-positive timeout did not panic")
		}
	}()
	prompt := func(context.Context, statemachine.Observation[obsState, obsEvent], int) {}
	statemachine.TimeoutObserver(prompt, 0)
}

func TestContainedObserverSeparatesObservationHealth(t *testing.T) {
	var mu sync.Mutex
	var reported []error
	panicking := func(context.Context, statemachine.Observation[obsState, obsEvent], int) {
		panic("observer exploded")
	}
	instance := statemachine.NewInstanceWithObservers(observerTable(), obsIdle,
		statemachine.ContainedObserver(panicking, func(err error) {
			mu.Lock()
			reported = append(reported, err)
			mu.Unlock()
		}))
	state, err := instance.Fire(context.Background(), obsGo, 0)
	if err != nil || state != obsRunning {
		t.Fatalf("contained Fire = %v, %v", state, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reported) != 2 { // one exit and one entry delivery both failed
		t.Fatalf("reported = %+v", reported)
	}
	for _, failure := range reported {
		if !errors.Is(failure, statemachine.ErrObserverFailed) {
			t.Fatalf("reported failure = %v", failure)
		}
	}
}

func TestContainedObserverContainsFailingReport(t *testing.T) {
	panicking := func(context.Context, statemachine.Observation[obsState, obsEvent], int) {
		panic("observer exploded")
	}
	instance := statemachine.NewInstanceWithObservers(observerTable(), obsIdle,
		statemachine.ContainedObserver(panicking, func(error) { panic("report exploded") }))
	if state, err := instance.Fire(context.Background(), obsGo, 0); err != nil || state != obsRunning {
		t.Fatalf("Fire with failing report = %v, %v", state, err)
	}
	if statemachine.ContainedObserver[obsState, obsEvent, int](nil, func(error) {}) != nil {
		t.Fatal("nil observer did not collapse to nil")
	}
}

func TestContainedTimeoutCompositionKeepsFireClean(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	blocked := func(context.Context, statemachine.Observation[obsState, obsEvent], int) {
		<-release
	}
	var mu sync.Mutex
	var reported []error
	instance := statemachine.NewInstanceWithObservers(observerTable(), obsIdle,
		statemachine.ContainedObserver(
			statemachine.TimeoutObserver(blocked, 10*time.Millisecond),
			func(err error) {
				mu.Lock()
				reported = append(reported, err)
				mu.Unlock()
			}))
	state, err := instance.Fire(context.Background(), obsGo, 0)
	if err != nil || state != obsRunning {
		t.Fatalf("composed Fire = %v, %v", state, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reported) == 0 {
		t.Fatal("timeout was not reported through the sink")
	}
	for _, failure := range reported {
		if !errors.Is(failure, statemachine.ErrObserverTimeout) {
			t.Fatalf("sink failure = %v", failure)
		}
	}
}

func TestCombinedObserversPanicWhenInvokedDirectly(t *testing.T) {
	combined := statemachine.Observers(
		func(context.Context, statemachine.Observation[obsState, obsEvent], int) { panic("inner") },
	)
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("combined observer did not transport the failure")
		}
		err, ok := recovered.(error)
		if !ok || !errors.Is(err, statemachine.ErrObserverFailed) {
			t.Fatalf("recovered = %+v", recovered)
		}
	}()
	combined(context.Background(), statemachine.Observation[obsState, obsEvent]{Seq: 1}, 0)
}
