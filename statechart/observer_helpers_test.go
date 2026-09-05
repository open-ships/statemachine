package statechart_test

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/open-ships/statemachine"
	"github.com/open-ships/statemachine/statechart"
)

func observerHierarchy() *statechart.Chart[testState, testEvent, *testData] {
	return statechart.MustCompile(definition{
		States:    baseStates(),
		Substates: hierarchy(),
		Initials: []statechart.Initial[testState]{
			{Parent: a, Child: a1}, {Parent: b, Child: b1},
		},
		Transitions: []transition{{From: a, Event: goB, To: b}},
	})
}

var errObserverPanic = errors.New("typed observer panic")

func panicFromStatechartObserver(context.Context, statechart.Observation[testState, testEvent], *testData) {
	panic(errObserverPanic)
}

func exitFromStatechartObserver(context.Context, statechart.Observation[testState, testEvent], *testData) {
	runtime.Goexit()
}

// observerEvidence finds original callback evidence inside the joined and
// nested errors produced by helper composition, rather than checking only the
// outer wrapper's stack.
func observerEvidence(err error, match func(*statemachine.ObserverError) bool) *statemachine.ObserverError {
	if failure, ok := err.(*statemachine.ObserverError); ok && match(failure) {
		return failure
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, inner := range joined.Unwrap() {
			if found := observerEvidence(inner, match); found != nil {
				return found
			}
		}
	} else if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return observerEvidence(wrapped.Unwrap(), match)
	}
	return nil
}

func TestObserverHelpersPreserveInfoOrderAndOriginalFailureStacks(t *testing.T) {
	type requestKey struct{}
	data := &testData{}
	var received []statechart.Observation[testState, testEvent]
	var contexts []context.Context
	var reported []error
	observer := statechart.ContainedObserver(
		statechart.TimeoutObserver(statechart.Observers(
			nil,
			panicFromStatechartObserver,
			func(ctx context.Context, observation statechart.Observation[testState, testEvent], got *testData) {
				received = append(received, observation)
				contexts = append(contexts, ctx)
				if got != data {
					panic("observer data identity changed")
				}
			},
			exitFromStatechartObserver,
		), time.Second),
		func(err error) { reported = append(reported, err) },
	)
	instance, err := observerHierarchy().NewWithObservers(a, observer)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), requestKey{}, "request")
	state, err := instance.Fire(ctx, goB, data)
	if state != b1 || err != nil || len(received) != 4 || len(reported) != 4 {
		t.Fatalf("Fire = %v, %v; observations %d, reports %d", state, err, len(received), len(reported))
	}
	var nodes []testState
	for index, observation := range received {
		nodes = append(nodes, observation.State)
		if observation.Seq != uint64(index+1) || observation.Step != 1 || observation.Run != 1 || observation.Remaining != uint64(3-index) || !observation.At.Equal(received[0].At) {
			t.Fatalf("observation[%d] ordering = %+v", index, observation)
		}
		if observation.Info.Source != a1 || observation.Info.Handler != a || observation.Info.Destination != b1 || observation.Info.Event != goB {
			t.Fatalf("observation[%d] Info = %+v", index, observation.Info)
		}
		if contexts[index].Value(requestKey{}) != "request" || !errors.Is(contexts[index].Err(), context.Canceled) {
			t.Fatalf("observation[%d] context = %v", index, contexts[index])
		}
		failure := reported[index]
		if !errors.Is(failure, statemachine.ErrObserverFailed) || !errors.Is(failure, errObserverPanic) {
			t.Fatalf("report[%d] = %v", index, failure)
		}
		panicked := observerEvidence(failure, func(e *statemachine.ObserverError) bool { return e.Value == errObserverPanic })
		if panicked == nil || panicked.Observer != 0 || panicked.Seq != observation.Seq || !strings.Contains(panicked.Stack, "panicFromStatechartObserver") {
			t.Fatalf("report[%d] original panic = %+v", index, panicked)
		}
		stopped := observerEvidence(failure, func(e *statemachine.ObserverError) bool { return e.Stopped })
		if stopped == nil || stopped.Observer != 2 || stopped.Seq != observation.Seq || !strings.Contains(stopped.Stack, "exitFromStatechartObserver") {
			t.Fatalf("report[%d] original Goexit = %+v", index, stopped)
		}
	}
	if !slices.Equal(nodes, []testState{a1, a, b, b1}) {
		t.Fatalf("nodes = %v", nodes)
	}
}

func TestContainedTimeoutObserverCancelsContextAndReportsTimeout(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	type delivery struct {
		ctx         context.Context
		observation statechart.Observation[testState, testEvent]
	}
	received := make(chan delivery, 4)
	var reported []error
	observer := statechart.ContainedObserver(statechart.TimeoutObserver(
		func(ctx context.Context, observation statechart.Observation[testState, testEvent], _ *testData) {
			received <- delivery{ctx, observation}
			<-release
		}, 10*time.Millisecond), func(err error) { reported = append(reported, err) })
	instance, err := observerHierarchy().NewWithObservers(a, observer)
	if err != nil {
		t.Fatal(err)
	}
	state, err := instance.Fire(context.Background(), goB, &testData{})
	if state != b1 || err != nil || len(reported) != 4 {
		t.Fatalf("Fire = %v, %v; reports %d", state, err, len(reported))
	}
	for range 4 {
		select {
		case got := <-received:
			if _, ok := got.ctx.Deadline(); !ok || got.ctx.Err() == nil || got.observation.Info.Destination != b1 {
				t.Fatalf("timed-out delivery = %+v, context error %v", got, got.ctx.Err())
			}
		case <-time.After(time.Second):
			t.Fatal("observer never started")
		}
	}
	for _, failure := range reported {
		if !errors.Is(failure, statemachine.ErrObserverFailed) || !errors.Is(failure, statemachine.ErrObserverTimeout) {
			t.Fatalf("timeout report = %v", failure)
		}
	}
}

func TestContainedObserverDropsReportFailures(t *testing.T) {
	for _, test := range []struct {
		name   string
		report func(error)
	}{
		{"nil", nil},
		{"panic", func(error) { panic("report panic") }},
		{"Goexit", func(error) { runtime.Goexit() }},
	} {
		t.Run(test.name, func(t *testing.T) {
			instance, err := observerHierarchy().NewWithObservers(a, statechart.ContainedObserver(panicFromStatechartObserver, test.report))
			if err != nil {
				t.Fatal(err)
			}
			if state, err := instance.Fire(context.Background(), goB, &testData{}); state != b1 || err != nil {
				t.Fatalf("Fire = %v, %v", state, err)
			}
		})
	}
}

func TestObserverHelpersConstruction(t *testing.T) {
	if statechart.Observers[testState, testEvent, *testData](nil) != nil ||
		statechart.TimeoutObserver[testState, testEvent, *testData](nil, time.Second) != nil ||
		statechart.ContainedObserver[testState, testEvent, *testData](nil, nil) != nil {
		t.Fatal("nil observers must collapse to nil")
	}
	for _, timeout := range []time.Duration{0, -time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("non-positive timeout did not panic")
				}
			}()
			statechart.TimeoutObserver(panicFromStatechartObserver, timeout)
		})
	}
}
