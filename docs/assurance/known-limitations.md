# Known limitations

- This module is not a safety controller and cannot terminate arbitrary Go code or external hardware work.
- A Journal prepares in-doubt state before Issue and receives a closure Snapshot after every commit, fault, and recovery, but it cannot atomically combine arbitrary controller I/O with durable process storage. Controllers still need fencing, expiry, and idempotency. A failed closure write leaves the journal conservatively stale: the next restoration faults and requires adjudication.
- Supervisor Recorder and Journal timeouts bound the caller's wait, not the lifetime of an adapter goroutine that ignores cancellation; each abandoned delivery can leak one goroutine. The same applies to observers wrapped by TimeoutObserver.
- Snapshot validation cannot detect rollback to a valid older Snapshot. Compare Revision, Attempt, and Restarts against an independently stored durable high-water mark before trusting a restored execution; Snapshots carry no signature or provenance binding.
- Record.At, Snapshot.RecordedAt, Fault.OccurredAt, and Observation.At are wall time and are subject to clock steps (NTP or GNSS discipline). Ordering across a step is not trustworthy; Record (Restarts, Seq) and Observation Seq are authoritative.
- Record CauseText and adjudication Evidence are free text. Machine-classifiable incident taxonomies need application-side error codes carried in the causes themselves.
- Statechart keeps Source committed on entry failure, but completed exit/entry actions and external effects are not rolled back. ActionError reports partial lifecycle progress.
- Observer failure is returned after commit. Callers must not interpret that error as a failed state change; ContainedObserver is the seam for callers that need a clean transition error channel.
- Runtime limits and Limits.MaxRecords bound admitted library work and retained history, not memory or work launched by application callbacks.
- Function values and mutable closure captures remain shared by compiled definitions. Callers must synchronize or avoid mutable captures.
- State, event, Snapshot, Record, and Observation values are shallow, and a retained Fault cause is an application-owned error value. Use immutable value types, immutable error values, and immutable evidence snapshots.
- Verification evidence is untyped: the module cannot validate sensor identity, evidence age, calibration, coherence, or operator authority. A CE-oriented integration profile must enforce typed evidence before any Check runs (INT-EVID-001).
- Existing historical tags through v1.2.1 are lightweight and unsigned. They are not rewritten; 1.3.0 and later use the signed, human-gated release process.
