# Next steps

## Production transactional Store

The `persist.Store` interface already defines an adapter-owned unit of work,
and `persist.FuncStore` plus the SQL example show how to pass `*sql.Tx` through
to a transition effect. The repository does not currently ship a production
database Store. `MemoryStore` provides optimistic in-memory state updates only;
it cannot roll back arbitrary effects.

- [ ] Decide which production database adapters or SQL dialects the library
  will support, or explicitly keep the seam application-owned.
- [ ] Implement a production Store that begins a transaction, loads state and
  revision, invokes the transition exactly once, conditionally writes the new
  state, and commits or rolls back.
- [ ] Ensure panics, transition errors, and version conflicts roll back the
  database transaction without retrying the transition effect.
- [ ] Add an outbox writer that uses the same transaction as the state update
  and enforces a stable command/idempotency key with a unique constraint.
- [ ] Treat cancellation and commit errors as potentially ambiguous outcomes;
  require reload/reconciliation before an application retry.
- [ ] Require idempotent outbox relays and consumers because delivery may still
  occur more than once.
- [ ] Document that direct network or hardware actions inside `Do` are not
  transactional. Use an outbox for network delivery and the supervised
  Issue–Verify protocol for physical actions.
- [ ] Add database integration and fault-injection tests covering atomic
  state/outbox commit, rollback, conflicts, ambiguous commit outcomes, and
  callback-at-most-once behavior.

### Acceptance criteria

- State and its outbox record become visible together or not at all.
- A failed or conflicting transaction leaves neither change committed.
- The Store never automatically repeats a transition effect.
- An unknown commit outcome is reported as unknown and reconciled by reload,
  never treated as proof that nothing happened.
