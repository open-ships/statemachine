package persist_test

import (
	"context"
	"errors"
	"math"
	"runtime"
	"testing"

	"github.com/open-ships/statemachine"
	"github.com/open-ships/statemachine/persist"
)

func TestContradictoryStoreSuccessIsRejected(t *testing.T) {
	failure := errors.New("effect failed")
	for _, test := range []struct {
		name        string
		effectError error
		returned    state
		want        error
	}{
		{"failed step", failure, paid, persist.ErrStepFailed},
		{"wrong state", nil, draft, persist.ErrStateMismatch},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := persist.FuncStore[string, state, struct{}]{UpdateFunc: func(ctx context.Context, _ string, step func(context.Context, state, struct{}) (state, error)) (state, error) {
				_, _ = step(ctx, draft, struct{}{})
				return test.returned, nil
			}}
			result, err := persist.Step(context.Background(), store, "A", machine(func(context.Context, *command) error { return test.effectError }), pay, nil)
			if !errors.Is(err, persist.ErrStoreContract) || !errors.Is(err, test.want) || result.Confirmed || !result.Attempted {
				t.Fatalf("Step = %+v, %v", result, err)
			}
			if test.effectError != nil && !errors.Is(err, failure) {
				t.Fatalf("transition failure lost: %v", err)
			}
		})
	}
}

func TestStoreCannotHideAnIncompleteCallback(t *testing.T) {
	for _, mode := range []string{"effect panic", "effect Goexit", "data panic"} {
		t.Run(mode, func(t *testing.T) {
			store := persist.FuncStore[string, state, struct{}]{UpdateFunc: func(ctx context.Context, _ string, step func(context.Context, state, struct{}) (state, error)) (state, error) {
				done := make(chan struct{})
				go func() {
					defer close(done)
					defer func() { _ = recover() }() // broken adapter swallows the panic
					_, _ = step(ctx, draft, struct{}{})
				}()
				<-done
				return draft, nil
			}}
			m := machine(func(context.Context, *command) error {
				if mode == "effect Goexit" {
					runtime.Goexit()
				}
				panic("effect failed")
			})
			result, err := persist.Step(context.Background(), store, "A", m, pay, func(struct{}) *command {
				if mode == "data panic" {
					panic("data failed")
				}
				return &command{}
			})
			if !errors.Is(err, persist.ErrStoreContract) || !errors.Is(err, persist.ErrStepIncomplete) || result.Confirmed {
				t.Fatalf("incomplete callback = %+v, %v", result, err)
			}
		})
	}
}

func TestStoreSuccessWithNaNStateIsRejected(t *testing.T) {
	m := statemachine.MustCompile([]statemachine.Transition[float64, int, struct{}]{{From: 0, Event: 1, To: 1}})
	store := persist.FuncStore[string, float64, struct{}]{UpdateFunc: func(ctx context.Context, _ string, step func(context.Context, float64, struct{}) (float64, error)) (float64, error) {
		if _, err := step(ctx, 0, struct{}{}); err != nil {
			return 0, err
		}
		return math.NaN(), nil
	}}
	result, err := persist.Step(context.Background(), store, "A", m, 1, nil)
	if !errors.Is(err, persist.ErrStateMismatch) || result.Confirmed {
		t.Fatalf("Step = %+v, %v", result, err)
	}
}

func TestMemoryStoreRejectsNaNInitialKeys(t *testing.T) {
	initial := map[float64]state{math.NaN(): draft}
	if store, err := persist.NewMemoryStoreChecked(initial); store != nil || !errors.Is(err, persist.ErrInvalidKey) {
		t.Fatalf("checked constructor = %v, %v", store, err)
	}
	defer func() {
		if got := recover(); got != persist.ErrInvalidKey {
			t.Fatalf("constructor panic = %v", got)
		}
	}()
	_ = persist.NewMemoryStore(initial)
}
