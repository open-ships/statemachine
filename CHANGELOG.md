# Changelog

## Unreleased

- Keep verification-expiry publication owned through Journal and Recorder
  delivery whether Verify or a timer detects the deadline. Recovery and
  adjudication cannot overtake publication, and timer expiry cannot release
  an enclosing Issue call that has not returned.
- Preserve the original Change and known Issue completion in synchronous
  verification-timeout results and records. Expiration timing is recorded
  separately from the original command's start time.
- Retain earlier committed Steps' observer failures, original causes, and
  stacks when a later queued callback exits with runtime.Goexit.
- Strengthen the queued fuzz model with exact event traces and effect counts,
  independent cursors, and explicit synchronous cancellation outcomes.
- Scan the separate SQLite integration module, including test dependencies,
  for known vulnerabilities in the full local and CI checks.

## 1.4.0 — first stable release

This version establishes the stable interface. Every earlier tag is a development
prerelease, including the ordinary numeric tags through 1.3.2. The module and
import path remain `github.com/open-ships/statemachine`; no compatibility shims
are provided for prerelease interfaces or snapshots.

- Give every Supervisor construction and restoration a fresh `IncarnationID`.
  Attempt and lifecycle Record identities include it, preventing identity reuse
  when a clean restart or guard refusal was not followed by a durable write.
  Snapshot schema 2 carries the new identity evidence and rejects old schemas.
- Keep each public Supervisor Operation live through its Recorder/Journal
  finalization and every detached callback. Recovery refuses overlap until
  that complete lifetime ends.
- Retain Journal ownership until a timed-out callback actually finishes, and
  refuse overlapping adapter delivery promptly with `ErrDeliveryBusy`. Stale
  writes cannot overtake newer snapshots, and Trip does not queue behind an
  unbounded backlog of Recorder deliveries. Adapter failures remain visible.
- Retain bounded lifecycle history in causal sequence order, preserve original
  Attempt timing and revision in `Record.Change`, and refuse exhausted Record
  counters before sequence wraparound. Adopting an external self-transition
  now commits and advances Revision just like successful Verification.
- Bound diagnostic text rendering so a custom Trip cause's `Error` method
  cannot block or panic during fault publication. Unknown error types use a
  type description while the original error identity remains available.
- Expand public-protocol regression coverage for late Journal writes, Recorder
  finalization, repeated restoration, recovery, record identity, and diagnostics.
- Reject unhashable dynamic keys and non-reflexive comparable keys such as NaN
  consistently across Machine, Statechart, and MemoryStore. A zero Statechart
  Instance safely refuses dynamic events that cannot be used as map keys.
- Reuse compiled hierarchy relationships for Statechart transition paths and
  Position inspection. Partial lifecycle diagnostics count only completed
  non-nil actions.
- Add typed Statechart `TimeoutObserver` and `ContainedObserver` helpers and
  share copying, ordered delivery, composition, and failure containment across
  execution models. Timeout callbacks receive a derived deadline context that
  is canceled after delivery. Document overlapping abandoned callbacks and
  unbounded report callbacks, and retain original nested panic/Goexit stacks.
- Add queued `Options`/`NewWithOptions` to combine custom limits and observers.
  Preserve original queued callback panic values while exposing bounded origin
  stack evidence through `Runtime.LastPanic`. Reuse the eager `Permitted`
  snapshot without a second copy.
- Add reusable `persist/persisttest` Store contract checks and a separate real
  SQLite integration module covering transactions, state/outbox atomicity,
  rollback, conflict handling, and an injected post-commit response failure.
  Strengthen Store ownership and callback validation without adding root-module
  dependencies. Applications still own production-database validation.
- Centralize local and CI assurance in scripts with pinned analysis tools, a
  90% race-tested coverage floor, dependency-graph verification, and SQLite
  integration. Use Go 1.26.8 for routine checks and releases, test Go 1.27.1
  compatibility, and keep Go 1.26.0 as the separately tested language minimum.
- Retain per-change and campaign fuzz failures, pass manual fuzz inputs through
  environment variables, and retain three 100 ms benchmark samples with
  allocation data instead of ten-iteration smoke timings.
- Pin the reviewed shared release workflow by commit and align provenance
  documentation with annotated tags, checksums, and artifact attestations.
  `VERSION` selects 1.4.0 as the next release baseline; later stable releases
  follow the compatibility policy in `CONTRIBUTING.md`.

## 1.3.2 — prerelease

- Replace the caller-owned, effectful `Machine.Fire` interface with pure
  `Machine.Next`. `Next` runs Guards and selects a destination but cannot run
  `Do`; transition effects now execute only through `Instance`, queued Runtime,
  or Store-backed state owners. This is a breaking interface change that removes
  the lost-transition hazard where an effect ran but its returned state was
  discarded.

## 1.3.0 — prerelease

This development release contained breaking changes relative to 1.2.1. Its
historical notes describe the intended behavior at the time; the Supervisor
identity, ordering, and finalization defects in that implementation are corrected
in 1.4.0. The previously drafted 2.0.0 was never tagged.

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
- Publish successful current-main builds through the shared Open Ships release policy as annotated releases with checksums, an SBOM, separate build-provenance and SBOM attestations, and retained assurance artifacts; use `VERSION` as the next baseline and increment patch versions thereafter.
