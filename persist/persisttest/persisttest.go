// Package persisttest supplies reusable Store contract checks for application
// adapters. The checks use the adapter directly, so persist.Fire's defensive
// checks cannot hide an adapter defect. Database isolation, conditional-write
// races, transactional effects, and ambiguous commits also need tests against
// the application's actual database and driver.
package persisttest

import (
	"context"
	"errors"
	"testing"

	"github.com/open-ships/statemachine/persist"
)

// Fixture describes a fresh Store containing Key at Initial, with Missing
// absent. Initial and Next must differ. Load must read committed state through
// a separate operation, not reuse the Update transaction. Factory callbacks
// should register resource cleanup with t.Cleanup.
type Fixture[K, S comparable, X any] struct {
	Store   persist.Store[K, S, X]
	Key     K
	Missing K
	Initial S
	Next    S
	Load    func(context.Context, K) (S, error)
}

// Run checks successful commits, callback errors and panics, missing keys, and
// cancellation before loading. It creates a fresh Fixture for every subtest.
// The suite deliberately does not require callbacks to overlap: both locking
// transactions and optimistic conditional-write adapters satisfy Store.
func Run[K, S comparable, X any](t *testing.T, factory func(*testing.T) Fixture[K, S, X]) {
	t.Helper()
	fixture := func(t *testing.T) Fixture[K, S, X] {
		t.Helper()
		f := factory(t)
		if f.Store == nil || f.Load == nil || f.Initial == f.Next || f.Key == f.Missing {
			t.Fatal("invalid Store fixture: need Store, Load, distinct states and distinct present/missing keys")
		}
		return f
	}
	checkState := func(t *testing.T, f Fixture[K, S, X], want S) {
		t.Helper()
		got, err := f.Load(context.Background(), f.Key)
		if err != nil || got != want {
			t.Fatalf("committed state = %v, %v; want %v", got, err, want)
		}
	}
	t.Run("commit_once", func(t *testing.T) {
		f := fixture(t)
		calls := 0
		got, err := f.Store.Update(context.Background(), f.Key, func(_ context.Context, from S, _ X) (S, error) {
			calls++
			if from != f.Initial {
				t.Errorf("loaded state = %v; want %v", from, f.Initial)
			}
			return f.Next, nil
		})
		if err != nil || got != f.Next || calls != 1 {
			t.Fatalf("Update = %v, %v; calls = %d", got, err, calls)
		}
		checkState(t, f, f.Next)
	})
	t.Run("callback_error_aborts", func(t *testing.T) {
		f := fixture(t)
		failure := errors.New("persisttest: callback failure")
		calls := 0
		_, err := f.Store.Update(context.Background(), f.Key, func(context.Context, S, X) (S, error) {
			calls++
			return f.Next, failure
		})
		if !errors.Is(err, failure) || calls != 1 {
			t.Fatalf("Update = %v; calls = %d", err, calls)
		}
		checkState(t, f, f.Initial)
	})
	t.Run("callback_panic_aborts", func(t *testing.T) {
		f := fixture(t)
		marker := &struct{}{}
		calls := 0
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			_, _ = f.Store.Update(context.Background(), f.Key, func(context.Context, S, X) (S, error) {
				calls++
				panic(marker)
			})
		}()
		if recovered != marker || calls != 1 {
			t.Fatalf("panic = %v; calls = %d", recovered, calls)
		}
		checkState(t, f, f.Initial)
	})
	t.Run("missing_key", func(t *testing.T) {
		f := fixture(t)
		called := false
		_, err := f.Store.Update(context.Background(), f.Missing, func(context.Context, S, X) (S, error) {
			called = true
			return f.Next, nil
		})
		if !errors.Is(err, persist.ErrNotFound) || called {
			t.Fatalf("missing key = %v; called = %v", err, called)
		}
		checkState(t, f, f.Initial)
	})
	t.Run("canceled_before_load", func(t *testing.T) {
		f := fixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		called := false
		_, err := f.Store.Update(ctx, f.Key, func(context.Context, S, X) (S, error) {
			called = true
			return f.Next, nil
		})
		if !errors.Is(err, context.Canceled) || called {
			t.Fatalf("canceled update = %v; called = %v", err, called)
		}
		checkState(t, f, f.Initial)
	})
}
