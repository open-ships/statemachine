package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/open-ships/statemachine"
	"github.com/open-ships/statemachine/persist"
	"github.com/open-ships/statemachine/persist/persisttest"
	"modernc.org/sqlite"
	sqlitecode "modernc.org/sqlite/lib"
)

func databaseError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return errors.Join(persist.ErrNotFound, err)
	}
	var failure *sqlite.Error
	if errors.As(err, &failure) {
		switch failure.Code() & 0xff {
		case sqlitecode.SQLITE_BUSY, sqlitecode.SQLITE_LOCKED:
			return errors.Join(persist.ErrConflict, err)
		}
	}
	return err
}

// applicationStore keeps the state write and every effect using *sql.Tx in
// one transaction. Its deferred rollback also runs when step panics.
func applicationStore(db *sql.DB) persist.FuncStore[string, string, *sql.Tx] {
	return persist.FuncStore[string, string, *sql.Tx]{UpdateFunc: func(ctx context.Context, key string, step func(context.Context, string, *sql.Tx) (string, error)) (string, error) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return "", databaseError(err)
		}
		defer func() { _ = tx.Rollback() }()
		var from string
		var version uint64
		if err := tx.QueryRowContext(ctx, "SELECT state, version FROM aggregates WHERE id = ?", key).Scan(&from, &version); err != nil {
			return "", databaseError(err)
		}
		next, err := step(ctx, from, tx)
		if err != nil {
			return from, databaseError(err)
		}
		if err := ctx.Err(); err != nil {
			return from, err
		}
		result, err := tx.ExecContext(ctx, "UPDATE aggregates SET state = ?, version = version + 1 WHERE id = ? AND version = ?", next, key, version)
		if err != nil {
			return from, databaseError(err)
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return from, err
		}
		if rows != 1 {
			return from, persist.ErrConflict
		}
		if err := tx.Commit(); err != nil {
			return from, databaseError(err)
		}
		return next, nil
	}}
}

func openDatabase(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "state.db")+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(4)
	for _, statement := range []string{
		"CREATE TABLE aggregates (id TEXT PRIMARY KEY, state TEXT NOT NULL, version INTEGER NOT NULL)",
		"CREATE TABLE outbox (command_id TEXT PRIMARY KEY, aggregate_id TEXT NOT NULL, event_type TEXT NOT NULL)",
		"INSERT INTO aggregates VALUES ('order-1', 'pending', 0)",
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func assertDatabase(t *testing.T, db *sql.DB, wantState string, wantVersion, wantOutbox int) {
	t.Helper()
	var state string
	var version, count int
	if err := db.QueryRow("SELECT state, version FROM aggregates WHERE id = 'order-1'").Scan(&state, &version); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM outbox").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if state != wantState || version != wantVersion || count != wantOutbox {
		t.Fatalf("database = (%s, revision %d, outbox %d); want (%s, %d, %d)", state, version, count, wantState, wantVersion, wantOutbox)
	}
}

func TestApplicationAdapterConformance(t *testing.T) {
	persisttest.Run(t, func(t *testing.T) persisttest.Fixture[string, string, *sql.Tx] {
		db := openDatabase(t)
		return persisttest.Fixture[string, string, *sql.Tx]{
			Store: applicationStore(db), Key: "order-1", Missing: "absent", Initial: "pending", Next: "paid",
			Load: func(ctx context.Context, key string) (string, error) {
				var state string
				err := db.QueryRowContext(ctx, "SELECT state FROM aggregates WHERE id = ?", key).Scan(&state)
				return state, databaseError(err)
			},
		}
	})
}

func TestStateAndOutboxShareTransaction(t *testing.T) {
	for _, mode := range []string{"success", "error", "panic", "cancellation"} {
		t.Run(mode, func(t *testing.T) {
			db := openDatabase(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			failure := errors.New("effect failed after outbox insert")
			calls := 0
			m := statemachine.MustCompile([]statemachine.Transition[string, string, *sql.Tx]{{
				From: "pending", Event: "pay", To: "paid",
				Do: func(ctx context.Context, tx *sql.Tx) error {
					calls++
					if _, err := tx.ExecContext(ctx, "INSERT INTO outbox VALUES ('command-1', 'order-1', 'order.paid')"); err != nil {
						return err
					}
					switch mode {
					case "error":
						return failure
					case "panic":
						panic(failure)
					case "cancellation":
						cancel()
					}
					return nil
				},
			}})
			var err error
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				_, err = persist.Fire(ctx, applicationStore(db), "order-1", m, "pay", func(tx *sql.Tx) *sql.Tx { return tx })
			}()
			if calls != 1 {
				t.Fatalf("effect called %d times", calls)
			}
			switch mode {
			case "success":
				if err != nil || recovered != nil {
					t.Fatalf("success = %v, panic %v", err, recovered)
				}
				assertDatabase(t, db, "paid", 1, 1)
			case "error":
				if !errors.Is(err, failure) {
					t.Fatalf("effect error = %v", err)
				}
			case "panic":
				if recovered != failure {
					t.Fatalf("panic = %v", recovered)
				}
			case "cancellation":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation = %v", err)
				}
			}
			if mode != "success" {
				assertDatabase(t, db, "pending", 0, 0)
			}
		})
	}
}

func TestOutboxCommandIDIsDurablyUnique(t *testing.T) {
	db := openDatabase(t)
	store := applicationStore(db)
	for range 2 {
		if _, err := store.Update(context.Background(), "order-1", func(ctx context.Context, _ string, tx *sql.Tx) (string, error) {
			_, err := tx.ExecContext(ctx, "INSERT INTO outbox VALUES ('command-1', 'order-1', 'order.paid') ON CONFLICT (command_id) DO NOTHING")
			return "paid", err
		}); err != nil {
			t.Fatal(err)
		}
	}
	assertDatabase(t, db, "paid", 2, 1)
}

// The database commit here is real; the lost response is deliberately injected
// afterward. This tests result semantics, not remote network-failure behavior.
func TestPostCommitErrorDoesNotProveRollback(t *testing.T) {
	db := openDatabase(t)
	inner := applicationStore(db)
	responseError := errors.New("injected response loss after successful commit")
	store := persist.FuncStore[string, string, *sql.Tx]{UpdateFunc: func(ctx context.Context, key string, step func(context.Context, string, *sql.Tx) (string, error)) (string, error) {
		next, err := inner.Update(ctx, key, step)
		if err != nil {
			return next, err
		}
		return next, responseError
	}}
	m := statemachine.MustCompile([]statemachine.Transition[string, string, *sql.Tx]{{
		From: "pending", Event: "pay", To: "paid",
		Do: func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO outbox VALUES ('command-1', 'order-1', 'order.paid')")
			return err
		},
	}})
	result, err := persist.Step(context.Background(), store, "order-1", m, "pay", func(tx *sql.Tx) *sql.Tx { return tx })
	if !errors.Is(err, responseError) || !result.Attempted || result.Confirmed || result.TransitionError != nil {
		t.Fatalf("ambiguous result = %+v, %v", result, err)
	}
	assertDatabase(t, db, "paid", 1, 1)
}

func TestConcurrentTransactionsNeverRetryEffects(t *testing.T) {
	db := openDatabase(t)
	store := applicationStore(db)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	var calls atomic.Int64
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for _, id := range []string{"command-a", "command-b"} {
		wait.Go(func() {
			_, err := store.Update(ctx, "order-1", func(ctx context.Context, from string, tx *sql.Tx) (string, error) {
				calls.Add(1)
				if from != "pending" {
					t.Errorf("both transactions must load pending; got %s", from)
				}
				ready <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
					return from, ctx.Err()
				}
				_, err := tx.ExecContext(ctx, "INSERT INTO outbox VALUES (?, 'order-1', 'order.paid')", id)
				return "paid", err
			})
			results <- err
		})
	}
	for range 2 {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal("transactions did not reach their callbacks")
		}
	}
	close(release)
	wait.Wait()
	close(results)
	var commits, conflicts int
	for err := range results {
		switch {
		case err == nil:
			commits++
		case errors.Is(err, persist.ErrConflict):
			conflicts++
		default:
			t.Fatalf("unexpected transaction error: %v", err)
		}
	}
	if commits != 1 || conflicts != 1 || calls.Load() != 2 {
		t.Fatalf("commits=%d conflicts=%d callback calls=%d", commits, conflicts, calls.Load())
	}
	assertDatabase(t, db, "paid", 1, 1)
}
