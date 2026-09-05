# Migrating to the first stable release

1.4.0 is the first stable release. Existing tags through 1.3.2 are retained as prerelease history, including the Machine.Fire removal in 1.3.2. They are not rewritten. From 1.4.0 onward, patch releases preserve compatibility and minor releases add compatible functionality; incompatible stable API changes require a major release and the appropriate Go module path.

The import path remains `github.com/open-ships/statemachine`.

## Flat execution

Replace pure `machine.Fire(ctx, state, event, data)` selection with `machine.Next(ctx, state, event, data)`. Next runs guards and never runs Do. If the old caller relied on Do, create an Instance and call its Fire, use queued.Runtime for follow-up events, or use persist.Fire/persist.Step for Store-owned state. Choose the owner according to where the authoritative state lives. Merely renaming an effectful call to Next would omit its effect.

## Supervised execution and storage

- Persist and transport all three AttemptID fields: ExecutionID, IncarnationID, and Sequence. Pass the returned Result.AttemptKey unchanged to Verify. Use (ExecutionID, IncarnationID, Seq) to deduplicate Records. IncarnationID is random and cannot order two writers or fence an actuator.
- SnapshotVersion is 2. Restore rejects older schemas. Quiesce old writers and migrate saved state with application reconciliation; do not just relabel a version-1 Snapshot. Old journals lack the new incarnation and original Change timing evidence. Preserve outstanding external command identities in the application audit and resolve them before starting new work. This library does not provide a generic migration that can establish physical truth.
- Restore gives each execution a fresh IncarnationID. Restarts is a saved counter and may repeat when the same Snapshot is restored twice. Sequence high-water marks remain useful within the lineage but do not establish Snapshot freshness.
- Snapshots returned by a restored Supervisor use its new top-level IncarnationID. Their Pending.Attempt retains the original external command's identity, which can name an older incarnation; Fault.Change and a later Adopt preserve that evidence. Do not rewrite pending command identities to the new incarnation.
- Journal now receives successful startup and accepted guard-refusal closures as well as prepare, commit, fault, and recovery snapshots. Make saves idempotent. Delivery timeouts retain callback ownership; later operations may return ErrBusy or ErrCallbackRunning until the callback ends.
- Journal and Recorder delivery fail with ErrDeliveryBusy when the sink is already running. Check Status.JournalError, RecorderError, RecorderFailures, JournalRunning, and RecorderRunning. Keep durable delivery/retry policy in the adapter or application; the Supervisor does not silently retry.
- Adopt and Override always commit and advance Revision, including same-state decisions. Inspect Record.Adjudication to distinguish them from Retain. For a selected Attempt and its Verify or Adopt outcome, Result.Change and Record.Change preserve the original command's revision and start time. Result.Revision and Record.Revision describe the outcome; Result.StartedAt/CompletedAt and Record.OperationStartedAt/OperationCompletedAt describe the operation. Records without a selected command may carry a zero or operation-specific Change.
- Restore rejects exhausted Revision and Restarts counters and Record counters without sufficient diagnostic headroom. An exhausted Attempt counter can restore, but new Issue calls fault instead of wrapping it. If RecordsExhausted becomes true after the remaining diagnostic headroom is consumed, further records are dropped without reusing Seq. Roll over under application control with reconciliation and a new execution lineage.

## Keys, adapters, and observers

NaN-bearing state, event, and MemoryStore keys now fail validation, including nested float and complex values. Use reflexive identifiers. `persist.NewMemoryStoreChecked` returns construction errors; the convenience NewMemoryStore panics on invalid initial keys.

Store implementations must return the successful callback destination and must not swallow its error, panic, or incomplete return. Run `persist/persisttest.Run` and database-specific transaction tests; the separate SQLite integration module provides an example.

Use queued.NewWithOptions to combine limits and observers. Existing constructors remain available. Runtime.LastPanic exposes a bounded originating stack while the original panic value still propagates.

TimeoutObserver now passes a derived context with a deadline and cancels it after completion. Statechart exposes the same TimeoutObserver and ContainedObserver conveniences as the flat execution modules. Observer failures remain post-commit errors. A timed-out observer can continue running; shared sinks must handle overlap, and a blocking ContainedObserver reporter needs an outer timeout if its wait must be bounded.
