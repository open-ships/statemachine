# SQLite Store integration

This separate test module exercises an application-owned `persist.FuncStore`
against a real SQLite database through `database/sql` and `modernc.org/sqlite`.
The root library keeps zero third-party dependencies. Run from the repository:

```sh
go -C integration/sqlite test -race ./...
```

The tests cover the reusable `persist/persisttest` adapter contract suite,
state/outbox atomic commit, rollback on effect error, panic and cancellation,
stable outbox command identities, and competing WAL transactions. SQLite can
report a stale write transaction as `SQLITE_BUSY`/`SQLITE_BUSY_SNAPSHOT`; the
sample maps those to `persist.ErrConflict` and never retries the callback.
Another test injects a response error after a real local commit, demonstrating
that `StepResult.Confirmed == false` does not establish rollback; the response
failure is synthetic and does not simulate a remote storage engine.

The adapter is test/example code, not a supported database driver abstraction.
Applications own schema, transaction isolation, database-specific conflict
classification, and operational retry policy. Run equivalent integration tests
against the actual production database. These local tests cannot establish the
outcome of a network failure during a remote commit; that remains ambiguous and
requires application reconciliation and durable idempotency.
