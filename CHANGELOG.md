# Changelog

## 1.3.0

This release is the API. The project is pre-adoption, so it stays in v1
versioning space and contains breaking changes relative to v1.2.1; the
previously drafted 2.0.0 was never tagged and its changes land here.

- Assign Supervisor lifecycle Record identity (ExecutionID, Restarts, Seq) atomically with the state decision it describes, so Seq order is causal order; bound the in-process Records history with `Limits.MaxRecords` and count evictions and Recorder failures in Status.
- Persist the Record high-water mark and a Restarts incarnation counter in Snapshots so a restored execution never reuses durable Record identity, and stamp Snapshots with a schema Version that Restore validates.
- Write Journal closure Snapshots after every commit, freshly latched Fault, and completed recovery, serialized in decision order; surface closure failure through `Status.JournalError` and a `RecordJournalError` without rewriting the completed outcome.
- Require a durable Journal by default for Machines with external Issue actions; `Options.Unjournaled` is the explicitly named waiver.
- Add `Supervisor.Adjudicate` with typed Decisions — retain the committed state, adopt an in-doubt Change's destination on controller evidence, or override to a declared minimum-risk state — validated by Reconcilers against the proposed state and recorded with verbatim Evidence.
- Record a secondary cause only for outcomes whose callback was genuinely abandoned at a deadline, fixing a race that could duplicate a delivered outcome as a secondary cause and read the application error concurrently.
- Document the Clock/Timer injection contract (non-blocking, asynchronous dispatch, no reentrancy) and Trip's bounded return-latency behavior.
- Return the committed active state from `statechart.Instance.Fire`, matching the flat Instance.
- Remove per-Fire reflection from compiled Machines; strictness is proved once at Compile.
- Add `TimeoutObserver` and `ContainedObserver` combinators so observer delivery can be time-bounded and observation health can be separated from the transition error channel; `errors.Is` reaches `ErrObserverTimeout` through the containment layers.
- Add a model-based fuzz suite across every package — supervisor lifecycle, power-loss restart, adversarial clock, concurrent schedules, recorder/journal fault injection, snapshot and definition fuzzing, root-machine and statechart reference models, queued cascade semantics, and store-contract violation — plus a scheduled sustained fuzz campaign in CI.

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
