# Known limitations

- This module is not a safety controller and cannot terminate arbitrary Go code or external hardware work.
- A Journal prepares in-doubt state before Issue; it cannot atomically combine arbitrary controller I/O with durable process storage. Controllers still need fencing, expiry, and idempotency.
- Supervisor Recorder timeouts bound the caller's wait, not the lifetime of a Recorder goroutine that ignores cancellation.
- Statechart keeps Source committed on entry failure, but completed exit/entry actions and external effects are not rolled back. ActionError reports partial lifecycle progress.
- Observer failure is returned after commit. Callers must not interpret that error as a failed state change.
- Runtime limits bound admitted library work, not memory or work launched by application callbacks.
- Function values and mutable closure captures remain shared by compiled definitions. Callers must synchronize or avoid mutable captures.
- State, event, Snapshot, Record, and Observation values are shallow. Use immutable value types and immutable evidence snapshots.
- Existing historical tags through v1.2.1 are lightweight and unsigned. They are not rewritten; v2 and later use the signed, human-gated release process.
