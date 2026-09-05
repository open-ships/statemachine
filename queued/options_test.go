package queued_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/open-ships/statemachine"
	"github.com/open-ships/statemachine/queued"
)

func TestOptionsCombinesLimitsAndObservers(t *testing.T) {
	var r *queued.Runtime[state, event, *command]
	var observations []statemachine.Observation[state, event]
	m := statemachine.MustCompile([]row{
		{From: 0, Event: start, To: 1, Do: func(ctx context.Context, data *command) error {
			if err := r.Enqueue(ctx, first, data); err != nil {
				return err
			}
			if err := r.Enqueue(ctx, first, data); !errors.Is(err, queued.ErrRunLimit) {
				t.Errorf("second follow-up = %v", err)
			}
			return nil
		}},
		{From: 1, Event: first, To: 2},
	})
	opts := queued.Options[state, event, *command]{
		Limits: queued.Limits{MaxRoots: 1, MaxRunEvents: 2},
		Observers: []statemachine.Observer[state, event, *command]{func(_ context.Context, o statemachine.Observation[state, event], _ *command) {
			observations = append(observations, o)
		}},
	}
	var err error
	r, err = queued.NewWithOptions(m, 0, opts)
	if err != nil {
		t.Fatal(err)
	}
	opts.Observers[0] = nil // construction owns its observer list
	if got, err := r.Fire(context.Background(), start, &command{}); err != nil || got != 2 {
		t.Fatalf("Fire = %v, %v", got, err)
	}
	if len(observations) != 4 || r.Status().Limits != opts.Limits {
		t.Fatalf("observations/limits = %d/%+v", len(observations), r.Status().Limits)
	}
	defaults, err := queued.NewWithOptions(m, 0, queued.Options[state, event, *command]{})
	if err != nil || defaults.Status().Limits.MaxRunEvents != queued.DefaultMaxRunEvents {
		t.Fatalf("default options = %v, %v", defaults, err)
	}
	if _, err := queued.NewWithOptions(m, 0, queued.Options[state, event, *command]{Limits: queued.Limits{MaxRoots: 1}}); !errors.Is(err, queued.ErrInvalidLimits) {
		t.Fatalf("partial limits = %v", err)
	}
}

func originatingPanicEffect(context.Context, *command) error { panic(panicMarker) }

var panicMarker = &struct{ message string }{"original panic"}

func TestLastPanicPreservesOriginAndOriginalValue(t *testing.T) {
	m := statemachine.MustCompile([]row{
		{From: 0, Event: start, To: 1, Do: originatingPanicEffect},
		{From: 0, Event: after, To: 1},
		{From: 1, Event: fail, To: 2, Guard: func(context.Context, *command) error { panic(panicMarker) }},
	})
	r := queued.New(m, state(0))
	if _, ok := r.LastPanic(); ok {
		t.Fatal("new Runtime has a panic record")
	}
	checkPanic := func(ev event) {
		t.Helper()
		defer func() {
			if got := recover(); got != panicMarker {
				t.Fatalf("panic = %v; original identity lost", got)
			}
		}()
		_, _ = r.Fire(context.Background(), ev, &command{})
	}
	checkPanic(start)
	first, ok := r.LastPanic()
	if !ok || first.Event != start || first.At.IsZero() || !strings.Contains(first.Stack, "originatingPanicEffect") || len(first.Stack) > 64<<10 {
		t.Fatalf("panic record = %+v, %v", first, ok)
	}
	if _, err := r.Fire(context.Background(), after, &command{}); err != nil {
		t.Fatal(err)
	}
	if got, _ := r.LastPanic(); got != first {
		t.Fatal("successful root erased panic evidence")
	}
	checkPanic(fail)
	if got, _ := r.LastPanic(); got.Event != fail || got.At.Before(first.At) {
		t.Fatalf("latest panic = %+v", got)
	}
	if !strings.Contains(first.Stack, "originatingPanicEffect") {
		t.Fatal("later panic mutated the earlier snapshot")
	}
}
