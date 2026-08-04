package persist_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/open-ships/statemachine"
	"github.com/open-ships/statemachine/persist"
)

type state string
type event string

const (
	draft state = "draft"
	paid  state = "paid"

	pay event = "pay"
)

type tx struct {
	used bool
}

type command struct {
	tx     *tx
	effect func() error
}

func machine(effect func(context.Context, *command) error) *statemachine.Machine[state, event, *command] {
	return statemachine.MustCompile([]statemachine.Transition[state, event, *command]{
		{From: draft, Event: pay, To: paid, Do: effect},
	})
}

func TestFireUsesStoreUnitAndCommitsState(t *testing.T) {
	store := persist.NewMemoryStore(map[string]state{"A": draft})
	var got persist.MemoryUnit[string]
	m := machine(func(_ context.Context, c *command) error {
		c.tx.used = true
		return nil
	})

	next, err := persist.Fire(context.Background(), store, "A", m, pay, func(unit persist.MemoryUnit[string]) *command {
		got = unit
		return &command{tx: &tx{}}
	})
	if err != nil || next != paid {
		t.Fatalf("Fire = %v, %v; want paid, nil", next, err)
	}
	if got.Key != "A" || got.Revision != 0 {
		t.Fatalf("unit = %+v", got)
	}
	snapshot, err := store.Load(context.Background(), "A")
	if err != nil || snapshot.State != paid || snapshot.Revision != 1 {
		t.Fatalf("Load = %+v, %v", snapshot, err)
	}
}

func TestFirePreservesStateOnTransitionError(t *testing.T) {
	want := errors.New("charge failed")
	store := persist.NewMemoryStore(map[string]state{"A": draft})
	m := machine(func(context.Context, *command) error { return want })

	got, err := persist.Fire(context.Background(), store, "A", m, pay, func(persist.MemoryUnit[string]) *command {
		return &command{}
	})
	if got != draft || err != want {
		t.Fatalf("Fire = %v, %v; want draft, original error", got, err)
	}
	snapshot, _ := store.Load(context.Background(), "A")
	if snapshot.State != draft || snapshot.Revision != 0 {
		t.Fatalf("snapshot = %+v; failed transition committed", snapshot)
	}
}

func TestFireWithNilMachineUsesZeroMachine(t *testing.T) {
	store := persist.NewMemoryStore(map[string]state{"A": draft})
	_, err := persist.Fire[string, state, event, struct{}](
		context.Background(), store, "A", nil, pay, nil,
	)
	if !errors.Is(err, statemachine.ErrNotPermitted) {
		t.Fatalf("err = %v, want ErrNotPermitted", err)
	}
}

func TestStepDistinguishesAttemptFromConfirmedCommit(t *testing.T) {
	t.Run("confirmed", func(t *testing.T) {
		store := persist.NewMemoryStore(map[string]state{"A": draft})
		result, err := persist.Step(
			context.Background(), store, "A", machine(nil), pay,
			func(unit persist.MemoryUnit[string]) *command { return &command{tx: &tx{}} },
		)
		if err != nil || !result.Attempted || !result.Confirmed ||
			result.From != draft || result.To != paid || result.Event != pay || result.TransitionError != nil {
			t.Fatalf("Step = %+v, %v", result, err)
		}
	})

	t.Run("transition error", func(t *testing.T) {
		sentinel := errors.New("declined")
		store := persist.NewMemoryStore(map[string]state{"A": draft})
		result, err := persist.Step(
			context.Background(), store, "A", machine(func(context.Context, *command) error { return sentinel }), pay,
			func(persist.MemoryUnit[string]) *command { return &command{} },
		)
		if err != sentinel || !result.Attempted || result.Confirmed ||
			result.From != draft || result.To != draft || result.TransitionError != sentinel {
			t.Fatalf("Step = %+v, %v", result, err)
		}
	})

	t.Run("store failure before attempt", func(t *testing.T) {
		sentinel := errors.New("load failed")
		store := persist.FuncStore[string, state, *tx]{
			UpdateFunc: func(context.Context, string, func(context.Context, state, *tx) (state, error)) (state, error) {
				return draft, sentinel
			},
		}
		result, err := persist.Step(
			context.Background(), store, "A", machine(nil), pay,
			func(*tx) *command { return &command{} },
		)
		if err != sentinel || result.Attempted || result.Confirmed {
			t.Fatalf("Step = %+v, %v", result, err)
		}
	})

	t.Run("store failure after successful attempt", func(t *testing.T) {
		store := persist.FuncStore[string, state, *tx]{
			UpdateFunc: func(ctx context.Context, _ string, step func(context.Context, state, *tx) (state, error)) (state, error) {
				if _, err := step(ctx, draft, &tx{}); err != nil {
					t.Fatal(err)
				}
				return draft, persist.ErrConflict
			},
		}
		result, err := persist.Step(
			context.Background(), store, "A", machine(nil), pay,
			func(*tx) *command { return &command{} },
		)
		if !errors.Is(err, persist.ErrConflict) || !result.Attempted || result.Confirmed ||
			result.From != draft || result.To != paid || result.TransitionError != nil {
			t.Fatalf("Step = %+v, %v", result, err)
		}
	})
}

func TestMemoryStoreConflictRunsEachCallbackOnce(t *testing.T) {
	store := persist.NewMemoryStore(map[string]state{"A": draft})
	var calls atomic.Int32
	ready := make(chan struct{}, 2)
	release := make(chan struct{})

	update := func() error {
		_, err := store.Update(context.Background(), "A", func(
			context.Context, state, persist.MemoryUnit[string],
		) (state, error) {
			calls.Add(1)
			ready <- struct{}{}
			<-release
			return paid, nil
		})
		return err
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Go(func() { errs <- update() })
	}
	<-ready
	<-ready
	close(release)
	wg.Wait()
	close(errs)

	var conflicts int
	for err := range errs {
		if errors.Is(err, persist.ErrConflict) {
			conflicts++
			var conflict *persist.ConflictError
			if !errors.As(err, &conflict) || conflict.Expected != 0 || conflict.Actual != 1 {
				t.Errorf("conflict = %#v", err)
			}
			if got := err.Error(); got != "persist: version conflict: expected revision 0, found 1" {
				t.Errorf("conflict text = %q", got)
			}
		} else if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if calls.Load() != 2 || conflicts != 1 {
		t.Fatalf("calls=%d conflicts=%d; want 2, 1", calls.Load(), conflicts)
	}
	snapshot, _ := store.Load(context.Background(), "A")
	if snapshot.Revision != 1 {
		t.Fatalf("revision = %d, want 1", snapshot.Revision)
	}
}

func TestMemoryStoreCancellationBeforeAndAfterCallback(t *testing.T) {
	store := persist.NewMemoryStore(map[string]state{"A": draft})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var called bool
	if _, err := store.Update(ctx, "A", func(context.Context, state, persist.MemoryUnit[string]) (state, error) {
		called = true
		return paid, nil
	}); !errors.Is(err, context.Canceled) || called {
		t.Fatalf("pre-cancel = %v, called=%v", err, called)
	}

	ctx, cancel = context.WithCancel(context.Background())
	got, err := store.Update(ctx, "A", func(context.Context, state, persist.MemoryUnit[string]) (state, error) {
		cancel()
		return paid, nil
	})
	if got != draft || !errors.Is(err, context.Canceled) {
		t.Fatalf("post-cancel = %v, %v", got, err)
	}
}

func TestMemoryStorePanicPropagatesWithoutCommit(t *testing.T) {
	store := persist.NewMemoryStore(map[string]state{"A": draft})
	panicValue := &struct{ message string }{"effect panic"}

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = store.Update(context.Background(), "A", func(
			context.Context, state, persist.MemoryUnit[string],
		) (state, error) {
			panic(panicValue)
		})
	}()
	if recovered != panicValue {
		t.Fatalf("recovered = %#v, want %#v", recovered, panicValue)
	}
	snapshot, err := store.Load(context.Background(), "A")
	if err != nil || snapshot.State != draft || snapshot.Revision != 0 {
		t.Fatalf("Load after panic = %+v, %v; want draft at revision 0", snapshot, err)
	}
}

func TestMissingAndUnconfiguredStores(t *testing.T) {
	store := persist.NewMemoryStore[string, state](nil)
	if _, err := store.Load(context.Background(), "A"); !errors.Is(err, persist.ErrNotFound) {
		t.Fatalf("Load err = %v", err)
	}
	if _, err := store.Update(context.Background(), "A", func(context.Context, state, persist.MemoryUnit[string]) (state, error) {
		return paid, nil
	}); !errors.Is(err, persist.ErrNotFound) {
		t.Fatalf("Update err = %v", err)
	}

	var funcs persist.FuncStore[string, state, *tx]
	if _, err := funcs.Update(context.Background(), "A", func(context.Context, state, *tx) (state, error) {
		return paid, nil
	}); !errors.Is(err, persist.ErrNoStore) {
		t.Fatalf("FuncStore err = %v", err)
	}

	var nilStore persist.Store[string, state, *tx]
	if _, err := persist.Fire(context.Background(), nilStore, "A", machine(nil), pay, func(*tx) *command { return nil }); !errors.Is(err, persist.ErrNoStore) {
		t.Fatalf("Fire err = %v", err)
	}

	var typedNil *persist.MemoryStore[string, state]
	if _, err := persist.Fire(
		context.Background(), typedNil, "A", machine(nil), pay,
		func(persist.MemoryUnit[string]) *command { return nil },
	); !errors.Is(err, persist.ErrNoStore) {
		t.Fatalf("Fire with typed-nil Store err = %v", err)
	}
}

func TestFuncStorePassesTransactionToMachine(t *testing.T) {
	unit := &tx{}
	var updates int
	store := persist.FuncStore[string, state, *tx]{
		UpdateFunc: func(ctx context.Context, key string, step func(context.Context, state, *tx) (state, error)) (state, error) {
			updates++
			if key != "A" {
				t.Fatalf("key = %q", key)
			}
			return step(ctx, draft, unit)
		},
	}
	m := machine(func(_ context.Context, c *command) error {
		if c.tx != unit {
			t.Fatal("effect did not receive store transaction")
		}
		c.tx.used = true
		return nil
	})

	next, err := persist.Fire(context.Background(), store, "A", m, pay, func(tx *tx) *command {
		return &command{tx: tx}
	})
	if err != nil || next != paid || !unit.used || updates != 1 {
		t.Fatalf("Fire = %v, %v; used=%v updates=%d", next, err, unit.used, updates)
	}
}
