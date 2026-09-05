package persist_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/open-ships/statemachine"
	"github.com/open-ships/statemachine/persist"
)

type fuzzState int
type fuzzEvent int

const (
	fuzzIdle fuzzState = iota
	fuzzRunning
)

const fuzzGo fuzzEvent = 0

// fuzzMachine counts effect executions so contract enforcement — the effect
// runs at most once per library call — can be asserted exactly.
func fuzzMachine(effects *int) *statemachine.Machine[fuzzState, fuzzEvent, int] {
	return statemachine.MustCompile([]statemachine.Transition[fuzzState, fuzzEvent, int]{
		{
			From: fuzzIdle, Event: fuzzGo, To: fuzzRunning,
			Do: func(context.Context, int) error {
				*effects++
				return nil
			},
		},
	})
}

// rogueStore violates the Store contract in byte-programmed ways: it may call
// step zero, one, or two times, run the call on a joined goroutine, and
// return an independent error or a fabricated success.
type rogueStore struct {
	stepCalls   int
	onGoroutine bool
	updateErr   error
}

func (s rogueStore) Update(
	ctx context.Context,
	key string,
	step func(context.Context, fuzzState, struct{}) (fuzzState, error),
) (fuzzState, error) {
	state := fuzzIdle
	var stepErr error
	run := func() {
		for range s.stepCalls {
			state, stepErr = step(ctx, fuzzIdle, struct{}{})
		}
	}
	if s.onGoroutine {
		var wg sync.WaitGroup
		wg.Go(run)
		wg.Wait()
	} else {
		run()
	}
	if s.updateErr != nil {
		return state, s.updateErr
	}
	return state, stepErr
}

var errRogueUpdate = errors.New("fuzz: rogue update failure")

// FuzzStoreContract drives persist.Step through every byte-reachable
// combination of contract violations and asserts the documented sentinel
// errors, the at-most-once effect guarantee, and Confirmed semantics.
func FuzzStoreContract(f *testing.F) {
	f.Add(uint8(0), false, false)
	f.Add(uint8(1), false, false)
	f.Add(uint8(2), true, false)
	f.Add(uint8(1), true, true)
	f.Add(uint8(2), false, true)
	f.Fuzz(func(t *testing.T, calls uint8, onGoroutine, updateFails bool) {
		store := rogueStore{stepCalls: int(calls % 3), onGoroutine: onGoroutine}
		if updateFails {
			store.updateErr = errRogueUpdate
		}
		effects := 0
		machine := fuzzMachine(&effects)
		result, err := persist.Step(
			context.Background(), store, "key", machine, fuzzGo,
			func(struct{}) int { return 0 },
		)

		if effects > 1 {
			t.Fatalf("transition effect ran %d times", effects)
		}
		switch store.stepCalls {
		case 0:
			if updateFails {
				if !errors.Is(err, errRogueUpdate) || result.Confirmed {
					t.Fatalf("no-call failing update = %+v, %v", result, err)
				}
			} else if !errors.Is(err, persist.ErrStoreContract) || !errors.Is(err, persist.ErrStepNotCalled) {
				t.Fatalf("no-call success = %+v, %v", result, err)
			}
		case 1:
			if !result.Attempted || result.From != fuzzIdle || result.To != fuzzRunning || result.TransitionError != nil {
				t.Fatalf("single call = %+v, %v", result, err)
			}
			if updateFails {
				if !errors.Is(err, errRogueUpdate) || result.Confirmed {
					t.Fatalf("single call failing update = %+v, %v", result, err)
				}
			} else if err != nil || !result.Confirmed {
				t.Fatalf("single call success = %+v, %v", result, err)
			}
		case 2:
			if !errors.Is(err, persist.ErrStoreContract) || !errors.Is(err, persist.ErrStepRepeated) || result.Confirmed {
				t.Fatalf("repeated call = %+v, %v", result, err)
			}
			if effects != 1 {
				t.Fatalf("repeated call ran the effect %d times", effects)
			}
		}
	})
}

// FuzzMemoryStoreSerialization fires byte-programmed concurrent events at one
// MemoryStore key. Whatever interleaving occurs, every outcome must be either
// a committed transition, a documented refusal, or a version conflict, and
// the final state must be a declared state whose revision equals the number
// of successful commits.
func FuzzMemoryStoreSerialization(f *testing.F) {
	f.Add(uint8(1))
	f.Add(uint8(3))
	f.Add(uint8(6))
	f.Fuzz(func(t *testing.T, workers uint8) {
		concurrency := int(workers%6) + 1
		table := []statemachine.Transition[fuzzState, fuzzEvent, int]{
			{From: fuzzIdle, Event: fuzzGo, To: fuzzRunning},
			{From: fuzzRunning, Event: fuzzGo, To: fuzzIdle},
		}
		machine := statemachine.MustCompile(table)
		store := persist.NewMemoryStore(map[string]fuzzState{"key": fuzzIdle})

		var wg sync.WaitGroup
		commits := make([]int, concurrency)
		for worker := range concurrency {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				for range 4 {
					_, err := persist.Fire(
						context.Background(), store, "key", machine, fuzzGo,
						func(persist.MemoryUnit[string]) int { return 0 },
					)
					switch {
					case err == nil:
						commits[worker]++
					case errors.Is(err, persist.ErrConflict):
					default:
						panic("undocumented memory store error: " + err.Error())
					}
				}
			}(worker)
		}
		wg.Wait()

		total := 0
		for _, count := range commits {
			total += count
		}
		snapshot, err := store.Load(context.Background(), "key")
		if err != nil {
			t.Fatal(err)
		}
		if int(snapshot.Revision) != total {
			t.Fatalf("revision %d != %d commits", snapshot.Revision, total)
		}
		want := fuzzIdle
		if total%2 == 1 {
			want = fuzzRunning
		}
		if snapshot.State != want {
			t.Fatalf("state %v after %d commits, want %v", snapshot.State, total, want)
		}
	})
}

// FuzzStoreSuccessConsistency checks fabricated successful adapter responses
// against the callback's independently recorded outcome.
func FuzzStoreSuccessConsistency(f *testing.F) {
	f.Add(false, uint8(1))
	f.Add(false, uint8(0))
	f.Add(true, uint8(1))
	f.Fuzz(func(t *testing.T, effectFails bool, returned uint8) {
		failure := errors.New("fuzz: transition failed")
		machine := statemachine.MustCompile([]statemachine.Transition[fuzzState, fuzzEvent, int]{{
			From: fuzzIdle, Event: fuzzGo, To: fuzzRunning,
			Do: func(context.Context, int) error {
				if effectFails {
					return failure
				}
				return nil
			},
		}})
		store := persist.FuncStore[string, fuzzState, struct{}]{UpdateFunc: func(ctx context.Context, _ string, step func(context.Context, fuzzState, struct{}) (fuzzState, error)) (fuzzState, error) {
			_, _ = step(ctx, fuzzIdle, struct{}{})
			return fuzzState(returned % 3), nil
		}}
		result, err := persist.Step(context.Background(), store, "key", machine, fuzzGo, nil)
		switch {
		case effectFails:
			if !errors.Is(err, persist.ErrStepFailed) || !errors.Is(err, failure) || result.Confirmed {
				t.Fatalf("failed callback with Store success = %+v, %v", result, err)
			}
		case returned%3 != uint8(fuzzRunning):
			if !errors.Is(err, persist.ErrStateMismatch) || result.Confirmed {
				t.Fatalf("contradictory destination = %+v, %v", result, err)
			}
		default:
			if err != nil || !result.Confirmed {
				t.Fatalf("consistent Store success = %+v, %v", result, err)
			}
		}
	})
}
