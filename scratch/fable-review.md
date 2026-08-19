# Codebase Evaluation — maritime roboticist, CE-grade lens

Reviewer perspective: maritime-oriented robotics, CE-marking / class-society integration
(IEC 61508-3 SOUP justification, ISO 13849, DNV-RU-SHIP Pt.4 Ch.9-style software assessment),
human safety and liability as first priority.

Scope reviewed: every source file in `statemachine`, `queued`, `statechart`, `persist`,
`supervised`, `internal/*`; `SAFETY.md`, `docs/assurance/*`, `docs/adr/*`, CI workflows.
Verification run locally: `go vet ./...` clean; `go test -race -count=1 ./...` all green
(177 test funcs, ~6.7k test lines vs ~5.5k source lines).

---

## Overall verdict

One of the most honest and disciplined "safety-adjacent" libraries reviewed. The critical
insight — a committed logical state is **not evidence an actuator moved** — is threaded
through the whole design (SAFETY.md, the Issue/Verify split, `Uncertain` latching, in-doubt
snapshots). The scope boundary ("not a safety controller; e-stop must not depend on our lock
or queue") is exactly the claim structure a notified body or class society wants from a COTS
component. The `docs/assurance/requirements.md` LIB-*→test traceability matrix and INT-*
integration obligations are rare and genuinely useful for a safety case.

Found: **one release-blocking defect, one genuine resource-safety gap, one undocumented
deadlock hazard**, plus feature gaps to weigh before betting a vessel architecture on it.

---

## Findings, ranked

### 1. 🔴 Release-blocking: v2.0.0 without a `/v2` module path
`VERSION` says `2.0.0`, `CHANGELOG.md` documents 2.0.0, git tags stop at `v1.2.1` — but
`go.mod:1` still reads `module github.com/open-ships/statemachine`. Go's semantic import
versioning **rejects** a `v2.0.0` tag on a module whose `go.mod` path doesn't end in `/v2`.
The signed release process described in the assurance docs will produce a tag `go get`
cannot resolve. Either the module path must become
`github.com/open-ships/statemachine/v2` (updating every internal import — `statemachine.go:10`,
`supervised/result.go:8`, etc.), or the release must stay in v1 space.

### 2. 🔴 Unbounded in-process `Records` history
`supervised/record.go:108` — `s.records = append(s.records, record)` with no cap, ring, or
trim. Every Start/Issue/Verify/Trip/Recover/expiry/secondary-cause appends forever. The
project's own assurance list demands "deterministic resource limits," and `queued` got finite
root/run limits in v2 — but a Supervisor on a months-long deployment (the maritime norm) is
a slow, unbounded memory leak, and `Records()` copies the whole slice each call. Needs a
bounded ring (Seq already provides authoritative ordering, so drop-oldest is sound) or at
minimum an entry in `known-limitations.md`. Currently neither bounded nor documented.

### 3. 🟠 Clock/Timer contract is undocumented and lock-coupled
`s.clock.Now()`, `s.clock.AfterFunc(...)`, and `timer.Stop()` are invoked **while holding
`s.mu`** (`supervised/execution.go:879`, `:975`, `:1065`, `:1184`). Consequences:

- A Clock that blocks (network time source, contended fake) blocks **`Trip`**, which needs
  `s.mu` — quietly undermining the SAFETY.md claim that Trip is the supervisory inhibition
  path.
- A test fake whose `AfterFunc` fires the callback synchronously deadlocks instantly
  (`verificationExpired` re-acquires `s.mu`), as does a fake that advances time under its own
  lock (lock-order inversion with `s.mu`).

`clock.go` documents that verification timers are notification-only, but not the real
contract: *Now/AfterFunc/Stop must be non-blocking and must never invoke callbacks
synchronously.* For a seam explicitly designed for injection, that contract belongs in the
interface docs — an assessor will ask.

Related, minor: `Trip` latches immediately under `s.mu` (good), but its *return* can stall
up to ~2× `RecorderTimeout` behind `recordMu` (`record.go:87-115`). Worth one sentence in
the Trip docs so callers don't put Trip on a latency-sensitive path expecting fast return.

### 4. 🟡 Redundant per-Fire reflection in the hot path
`statemachine.go:223` runs `keycheck.Value(from)` and `keycheck.Value(event)` on **every**
`Fire`. Strictness is a property of the *type*, and `Compile` already proved it via
`StrictType` — for any compiled Machine these checks can never fail; they exist only to
protect the zero Machine that bypassed `Compile`. Each is a reflection call with a likely
heap escape, twice per transition, forever. Store a `strict bool` at compile time and only
value-check when false. Not a correctness issue, but free latency in a control loop.

### 5. 🟡 Wall-clock time in evidence, GPS/NTP steps
Deadline math is monotonic-safe (`clock.Now().Add(...)` / `.Before(...)` preserve the
monotonic reading, and restored in-doubt snapshots never resume timers — correctly latched
Faulted instead). But `Observation.At`, `Record.At`, `Snapshot.RecordedAt` are wall time,
and vessels see real clock steps (GPS discipline, NTP after link loss). Fine and
conventional for incident correlation — but integration guidance should note that `At`
ordering across a clock step is not trustworthy; `Seq` is authoritative (which
`record.go:44` does state — good).

### 6. Edge: `StepResult` race on a contract-violating Store
`persist/persist.go:156-187` — detection of double-invoke/no-invoke is excellent (CAS +
`firstDone` join handles a Store that calls `step` from another goroutine). But a truly
rogue Store that calls `step` again *after* `Update` returns writes
`result.From/To/TransitionError` while the caller reads the returned `StepResult` — a data
race outside the detection window. Best-effort is reasonable; a one-line doc note
("detection covers calls made before Update returns") would close it.

---

## Does it do everything a maritime roboticist needs?

**In scope and present, done well:** mandatory non-bypassable checks with correct
Guard-vs-interlock separation (SAFETY.md is *right* that guards route and must never be
interlocks — a distinction most FSM libraries botch); split Issue/Verify with fresh-evidence
seam; first-cause fault latching with secondary-cause recording; in-doubt journaling
*before* Issue; non-reusable Execution/Attempt identity for fencing correlation;
counter-exhaustion checks (`ErrCounterExhausted`); reconcile-before-start/recover;
`CallbackRunning` visibility for un-killable Go callbacks; deep Restore validation
(`supervised/execution.go:186-262`).

**Absent — decide if you can live without:**

- **No hierarchical supervised machine.** `supervised` is flat-only; `statechart` has
  hierarchy but no mandatory checks, budgets, or fault latching. Maritime mode structures
  (DP2 ⊃ auto-heading ⊃ …) must be flattened into a supervised definition or split across
  Supervisors. Real modeling cost.
- **No parallel/orthogonal regions and no history states** in `statechart`. Propulsion ×
  steering × nav-lights as independent concurrent regions means N separate Instances with
  app-level coordination.
- **No timed states / dwell limits.** Operation and verification budgets exist, but "must
  leave `Maneuvering` within 30 s or fault" needs an app-side timer firing an event. Common
  IEC 61508-style supervisory requirement; it will be rebuilt repeatedly.
- **No event priority or preemption.** `queued` is strictly FIFO; the only preemption is
  `Trip`→Fault. Coherent with "e-stop lives outside," but degraded-mode transitions
  competing with a deep queue have no fast lane.
- **No graceful abort of a pending Verify.** Operator abort = Trip → Fault → Recover.
  Workable and conservative, but the recovery ceremony for a routine abort may irritate
  operators; decide whether that's a feature (audit trail) or friction.
- **No snapshot schema versioning.** `Snapshot` carries `DefinitionID` but no format-version
  field; the docs demand "explicit Snapshot migration" yet the type gives migrations nothing
  to key on. Cheap to add now, painful after fielded journals exist.

All consistent with stated scope — but for a CE-marked product the dwell-timer, priority,
and mode-hierarchy layers land in application code, inside your safety case.

---

## Immutability and observability

**Immutability: exemplary.** Compiled definitions are deep-copied at construction and
*owned by value* inside executions (`instance.go:72`, `queued/queued.go:125`,
`supervised/execution.go:140`) so a caller overwriting `*machine` can't mutate a live
execution — tested (LIB-DEF-001). Faults are snapshotted on every exposure so no caller can
mutate the retained first cause (`faultRecord.snapshot`, tested by LIB-SUP-005). `Records()`
returns a copy. `known-limitations.md` honestly flags the residual hole (shared function
values / mutable closure captures / shallow T) — the correct place, since Go can't enforce it.

**Observability: strong, with the right semantics.** Observations fire only on *commit*,
never on attempt (correct — a census must never count uncommitted motion);
construction/restoration emit nothing and docs warn a fleet census must seed independently.
Seq/Step/Run/Remaining gives batch-complete detection. Observer panic/Goexit containment
preserves original stacks and reports post-commit as `ErrObserverFailed` without
un-committing state — the correct liability posture (the state change happened; the witness
failed). `Status` exposes `CallbackRunning` and `RecorderError`. Gaps: the `Records` bound
(finding #2), and `Record.CauseText` is free text — fine for forensics, but
machine-classifiable incident taxonomies need app-side error codes.

---

## Abnormal design patterns (and whether they're justified)

| Pattern | Verdict |
|---|---|
| Value-copy of compiled Machine into executions | Abnormal (Go norm: store the pointer) but deliberate, tested, cheap. Keep. |
| Goroutine-per-callback in `invoke`/`observer.Call` to contain `runtime.Goexit` | Very unusual; the only way to survive Goexit in Go. Cost: every guard/check/effect runs off the caller's goroutine (scheduling jitter, no goroutine-locals). Justified for this scope. |
| `Observers(...)` combinator communicating inner failures via `panic(errors.Join(...))` (`observe.go:111`) | Works only because delivery re-contains it; a user invoking the combined observer directly gets a surprise panic. Undocumented — add one sentence or return-based plumbing. |
| Returning `(Result, error)` where `err == result.Err` | Redundant by Go norms, but the errcheck rationale is stated and sound for this domain. Keep. |
| Type-erased execution context in `queued` (`execution.enqueue func(ctx, any, any) error`, `assign[T]`) | The `assign` failure path is unreachable in practice (owner check precedes it) — defensive dead code, mildly smelly but harmless. |
| Inconsistencies: `statechart.Instance.Fire` returns only `error` while flat `Instance.Fire` returns `(S, error)`; `sync.RWMutex` in statechart vs `Mutex` in Instance; `root[E, T, S]` type-param order | Cosmetic, but for a v2 API freeze reconcile the `Fire` signature asymmetry now. |

---

## Go idiom scorecard

**Excellent:**
- Useful zero values that fail safe (zero Machine *refuses* everything — the right default
  polarity for this domain).
- Sentinel errors with `Unwrap() []error` multi-error trees, used correctly and — unusually —
  with documented sentinel-travel hazards (`statemachine.go:27-30`).
- Modern `iter.Seq2` iterators with eager-vs-lazy semantics explicitly distinguished between
  `Machine.Permitted` and `Instance.Permitted`.
- `MustCompile` mirroring `regexp`, with correct "literals are program text, generated tables
  are input" guidance.
- No global state (v2 killed ambient `Enqueue`); `internal/` packages for shared invariants.
- `errors.Join` compile diagnostics reporting *all* defects at once.
- Zero dependencies verified in CI; CI actions pinned by SHA; race detector, coverage floor,
  govulncheck, fuzz, and benchmarks in the pipeline.
- Doc comments are reference-grade — every hazard a user will hit is written down at the API
  element where they'll hit it.

**Deductions:** per-Fire reflection (#4), unbounded records slice (#2), undocumented Clock
contract (#3), and the module-path defect (#1) — the one outright violation of Go ecosystem
rules.

---

## Bottom line

As the coordination/orchestration layer above an independent hazard-rated safety chain — the
only role it claims — this is acceptable for a maritime system's software bill of materials,
and the assurance documentation would actively help the certification file. Fix the `/v2`
module path before tagging, bound or document the `Records` growth, and write down the Clock
contract; then the remaining questions are architectural fit (flat-only supervision, no
parallel regions, no dwell timers), not quality.
