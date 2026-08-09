# Own Supervisor operations and durable Attempt identity

Start, Issue, Verify, and Recover each acquire a unique Operation token. Every callback and terminal continuation validates that token. Trip revokes and cancels an Operation but does not release its ownership until both the public call and all callbacks finish. Recover is unavailable while any prior Operation remains live.

An Attempt is identified by Execution ID plus sequence. Snapshot persists the Execution ID, Attempt high-water mark, and the typed identity and timing of any in-doubt Change. Restore never treats that state as clean or replays it: it restores Faulted and requires Recover reconciliation. Applications issuing external commands can require a Journal that saves the in-doubt Snapshot before Issue runs.

This deepens the Supervisor Module without reversing ADR-0001: other state owners keep their distinct execution and failure semantics.
