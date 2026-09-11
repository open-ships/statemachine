package queued_test

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/open-ships/statemachine"
	"github.com/open-ships/statemachine/queued"
)

func TestObserverFailureSurvivesLaterGoexit(t *testing.T) {
	var execution *queued.Runtime[int, int, struct{}]
	observerFailure := errors.New("census sink failed")
	machine := statemachine.MustCompile([]statemachine.Transition[int, int, struct{}]{
		{From: 0, Event: 0, To: 1, Do: func(ctx context.Context, data struct{}) error {
			return execution.Enqueue(ctx, 1, data)
		}},
		{From: 1, Event: 1, To: 2, Do: func(context.Context, struct{}) error {
			runtime.Goexit()
			return nil
		}},
	})
	execution = queued.NewWithObservers(machine, 0, func(context.Context, statemachine.Observation[int, int], struct{}) {
		panic(observerFailure)
	})
	state, err := execution.Fire(context.Background(), 0, struct{}{})
	if state != 1 || !errors.Is(err, queued.ErrExecutionStopped) {
		t.Fatalf("state=%d error=%v", state, err)
	}
	if !errors.Is(err, statemachine.ErrObserverFailed) || !errors.Is(err, observerFailure) {
		t.Fatalf("earlier committed step lost observer failure: state=%d error=%v", state, err)
	}
	var failure *statemachine.ObserverError
	if !errors.As(err, &failure) || !strings.Contains(failure.Stack, "TestObserverFailureSurvivesLaterGoexit") {
		t.Fatalf("observer origin stack was lost: %v", err)
	}
}
