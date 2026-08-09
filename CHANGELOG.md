# Changelog

## 2.0.0

- Give Supervisor Operations unique ownership tokens and block Recover until revoked work fully ends.
- Make verification deadlines admission checks, preserve callback violations alongside timeouts, and distinguish caller deadlines.
- Add durable Execution/Attempt identity, typed in-doubt Change snapshots, optional required Journal, deterministic Clock, Lifecycle Records, and bounded Recorder delivery.
- Return ordinary errors from Supervisor operations in addition to detailed Results.
- Prevent outward Fault mutation and retain panic stacks and secondary causes.
- Own compiled definitions inside Runtime, Statechart Instance, and Supervisor.
- Replace ambient package-level Enqueue with Runtime-bound Enqueue, canceled-root removal, and finite root/Run limits.
- Publish Statechart Destination only after entry succeeds and report partial lifecycle progress.
- Detect Store callback contract violations and unsafe generic map keys.
- Timestamp committed Observations and return contained Observer failures after commit with their original stacks.
- Add supervisor model fuzzing, logical/external benchmarks, static analysis, coverage retention, and requirements traceability.
- Replace automatic patch releases with signed, annotated, human-gated releases and retained assurance artifacts.
