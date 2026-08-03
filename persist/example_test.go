package persist_test

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/open-ships/statemachine"
	"github.com/open-ships/statemachine/persist"
)

// MemoryStore demonstrates optimistic versioning. Its unit carries revision
// metadata; unlike a database transaction, it cannot roll back arbitrary
// effects.
func ExampleFire() {
	type State string
	type Event string

	const (
		Draft State = "draft"
		Paid  State = "paid"
		Pay   Event = "pay"
	)

	type Unit = persist.MemoryUnit[string]
	machine := statemachine.MustCompile([]statemachine.Transition[State, Event, Unit]{
		{
			From: Draft, Event: Pay, To: Paid,
			Do: func(_ context.Context, unit Unit) error {
				fmt.Println("effect in unit:", unit.Key, unit.Revision)
				return nil
			},
		},
	})
	store := persist.NewMemoryStore(map[string]State{"order-1": Draft})

	next, err := persist.Fire(
		context.Background(), store, "order-1", machine, Pay,
		func(unit Unit) Unit { return unit },
	)
	snapshot, _ := store.Load(context.Background(), "order-1")
	fmt.Println(next, snapshot.Revision, err)

	// Output:
	// effect in unit: order-1 0
	// paid 1 <nil>
}

// sqlStore demonstrates the contract expected of an external adapter. The
// transition callback receives the same transaction that persists state, so it
// can also insert an outbox row. The version predicate prevents a lost update,
// and the callback is never retried automatically.
func sqlStore[S comparable](db *sql.DB, scanState func(*sql.Row) (S, uint64, error), encodeState func(S) any) persist.FuncStore[string, S, *sql.Tx] {
	return persist.FuncStore[string, S, *sql.Tx]{
		UpdateFunc: func(ctx context.Context, id string, step func(context.Context, S, *sql.Tx) (S, error)) (S, error) {
			var zero S
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return zero, err
			}
			defer tx.Rollback()

			from, version, err := scanState(tx.QueryRowContext(ctx,
				`SELECT state, version FROM aggregates WHERE id = ?`, id))
			if err != nil {
				return zero, err
			}
			next, err := step(ctx, from, tx)
			if err != nil {
				return from, err
			}
			result, err := tx.ExecContext(ctx,
				`UPDATE aggregates SET state = ?, version = version + 1 WHERE id = ? AND version = ?`,
				encodeState(next), id, version)
			if err != nil {
				return from, err
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return from, err
			}
			if rows != 1 {
				return from, persist.ErrConflict
			}
			if err := tx.Commit(); err != nil {
				return from, err
			}
			return next, nil
		},
	}
}

// A transactional FuncStore passes the same transaction to the transition
// effect and the conditional state write. The stable command ID prevents an
// application retry from enqueueing a second outbox record; delivery remains
// at-least-once, so the relay and consumer must still be idempotent.
func ExampleFuncStore() {
	type State string
	type Event string
	type Command struct {
		Tx        *sql.Tx
		OrderID   string
		CommandID string
	}

	const (
		Pending State = "pending"
		Paid    State = "paid"
		Pay     Event = "pay"
	)

	machine := statemachine.MustCompile([]statemachine.Transition[State, Event, *Command]{
		{
			From: Pending, Event: Pay, To: Paid,
			Do: func(ctx context.Context, command *Command) error {
				_, err := command.Tx.ExecContext(ctx, `
					INSERT INTO outbox (command_id, aggregate_id, event_type)
					VALUES (?, ?, 'order.paid')
					ON CONFLICT (command_id) DO NOTHING`,
					command.CommandID, command.OrderID)
				return err
			},
		},
	})

	var db *sql.DB // supplied by the application
	store := sqlStore(db,
		func(row *sql.Row) (State, uint64, error) {
			var state State
			var version uint64
			err := row.Scan(&state, &version)
			return state, version, err
		},
		func(state State) any { return state },
	)

	pay := func(ctx context.Context, orderID, commandID string) (State, error) {
		return persist.Fire(ctx, store, orderID, machine, Pay, func(tx *sql.Tx) *Command {
			return &Command{Tx: tx, OrderID: orderID, CommandID: commandID}
		})
	}
	_ = pay // invoked by the application's command handler
}
