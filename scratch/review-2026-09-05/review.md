Review of commit `9538650` (`v1.3.2`), September 5, 2026.

This is a well-designed small state-machine library with unusually careful concurrency contracts and substantial verification. Its flat selection and ordinary state-owning execution are its strongest parts. The main concern is that the Supervisor's durability and lifecycle-record guarantees exceed what its implementation currently ensures. Several failures reproduce despite all existing checks passing.

The review covered every production package, tests and fuzz models, examples, domain documentation, ADRs, assurance claims, CI, local release tags, and the exact `v1.1.0` shared release workflow from the neighboring `open-ships/ci` checkout. It did not exercise a live database, controller, or hosted GitHub release. Production source and existing tests were not changed. This directory contains review evidence only.

The repository contains 6,217 lines across 22 production Go files and 9,602 lines across 28 test files, counting comments and blank lines. There are 181 named tests, 16 fuzz targets, 18 benchmarks, and 18 example functions. Size is modest; most complexity is concentrated in supervised execution. Reproduction instructions and source are in [REPRODUCE.md](REPRODUCE.md).

**Verification completed.** The following passed:

- `go test ./...` with coverage: 94.1% total statement coverage.
- `go test -race ./...` and `go vet ./...`.
- The pinned errcheck and staticcheck commands from CI.
- Formatting inspection with `gofmt -l .`.
- All 16 fuzz targets, each with a three-second live run and two workers; an additional Statechart execution fuzz run passed for 15 seconds.
- All existing benchmarks with allocation reporting and 100 ms per benchmark.
- `go test ./...` under Go 1.26.8 and Go 1.27.1, in addition to the repository's Go 1.26.0 baseline.
- Focused reproductions in an isolated checkout under the race detector. These probes assert the defective behavior; their passing is evidence that the defects occur, not evidence of a fix.

Govulncheck reported no reachable vulnerable symbols. Its verbose output identified three package-level and 30 module-level standard-library advisories that it did not find reachable from this library. This is different from saying that Go 1.26.0 has no known vulnerabilities.

| Package | Statement coverage | Assessment |
|---|---:|---|
| Root `statemachine` | 97.2% | Small, clear, efficient; generic-key edge cases remain |
| `persist` | 97.6% | Sound documented Store contract; real database validation remains external |
| `queued` | 95.9% | Strong scheduler behavior; configuration and diagnostics need polish |
| `statechart` | 96.1% | Deliberate hierarchy and lifecycle semantics; zero-value key validation hole |
| `supervised` | 92.2% | Strong intent and checks; substantive durability and audit defects |
| Internal helpers | 100% | Useful small implementations; coverage does not imply full input coverage |

**Fix these correctness and compatibility problems first.** P1 means a high-priority reliability or compatibility defect; P2 means a concrete defect with more limited impact or conditions; P3 means a small diagnostic inconsistency. These are prioritization labels, not claims of external exploitability.

1. **P1 — A timed-out Journal closure can overwrite a newer successful preparation and reuse an externally issued Attempt ID.**

   In [supervised/execution.go:727](/Users/jacobthomas/code/openships/statemachine/supervised/execution.go:727), `journalMu` covers the caller's bounded wait. [supervised/record.go:221](/Users/jacobthomas/code/openships/statemachine/supervised/record.go:221) abandons a callback when the timeout expires. The mutex is then released even though the Journal write may still finish.

   Reproduced sequence: verify Attempt 1; delay its clean closure until its timeout; issue Attempt 2 and successfully save its in-doubt preparation; release the old closure. The Journal now holds the clean Attempt-1 snapshot. Restore and Start accept it, and the next Issue reuses the exact Execution ID and sequence already used by Attempt 2.

   The reproduction Journal is synchronized and repeated saves are idempotent. It can complete after cancellation, which the known limitations explicitly allow. The defect is the loss of the Journal ordering promised in [supervised/record.go:109](/Users/jacobthomas/code/openships/statemachine/supervised/record.go:109) and LIB-SUP-013. Documenting an abandoned goroutine does not make its stale durable write harmless.

   Repair direction: retain Journal ownership until the callback actually ends and prevent new preparations from overtaking it, or define and enforce conditional durable writes that reject older versions. Preserve a bounded caller response without releasing permission for an old write to corrupt newer truth. Add the complete late-closure/prepare/crash/restore sequence as a regression.

2. **P1 — Clean restarts can reuse durable Record identities.**

   [supervised/execution.go:332](/Users/jacobthomas/code/openships/statemachine/supervised/execution.go:332) increments `Restarts` only in memory. A successful Start stamps and records its outcome, but [finishReady:1435](/Users/jacobthomas/code/openships/statemachine/supervised/execution.go:1435) does not mark it durable, so Start's closure path does not save the new incarnation.

   Reproduced with cooperative adapters: save a clean verified commit; Restore and Start; crash; Restore the actual latest saved Journal snapshot and Start again. Both starts deliver the same `(ExecutionID, Restarts=1, Seq=4)` to Recorder. A sink using the documented identity as a unique key can discard or overwrite legitimate history. This requires neither caller-supplied rollback nor concurrent writers.

   Repair direction: durably reserve a new incarnation before emitting its first Record, or establish a separately unique incarnation identity. Saving after Recorder delivery still leaves a crash window. The restart model must retain all identities already delivered across incarnations and reject reuse. Its current [comparison against the last Journal snapshot](/Users/jacobthomas/code/openships/statemachine/supervised/model_fuzz_test.go:518) cannot detect this failure.

   A related, separately reproduced P2 gap affects accepted but refused Attempts. [execution.go:561](/Users/jacobthomas/code/openships/statemachine/supervised/execution.go:561) allocates an Attempt before selection, but an all-guards-declined outcome does not save its high-water mark. After a normal crash/restore, the next successful Issue can reuse that refused Attempt's ID. The refused Attempt did not issue an external command, so its immediate impact is weaker than finding 1; it still contradicts the non-reuse contract for accepted Attempts.

3. **P1 — A patch release removes a public method.**

   Local tag inspection shows `v1.3.1` exports `Machine.Fire`, while `v1.3.2`, which points to the reviewed commit, removes it and exports `Machine.Next`. An existing consumer can fail to compile after a patch update. [CHANGELOG.md:5](/Users/jacobthomas/code/openships/statemachine/CHANGELOG.md:5) acknowledges the breaking change but still labels it Unreleased. Its v1.3.0 entry also explicitly contains breaks relative to v1.2.1.

   The pure-selection decision in ADR-0004 is reasonable and should be preserved. The release policy needs to express that decision correctly. The exact shared workflow automatically increments a patch unless `VERSION` establishes a higher baseline; this repository leaves its baseline at 1.3.0 and has no exported-interface compatibility gate.

   Repair direction: publish future incompatible interfaces through an appropriate major module version, with migration instructions and compatibility checks against the prior release. Do not rewrite already published tags. Go's documented patch/minor compatibility expectations apply regardless of a project's informal pre-adoption status. See [Go module version numbering](https://go.dev/doc/modules/version-numbers).

4. **P2 — Adjudicating an external self-transition skips its commit/revision semantics.**

   [supervised/execution.go:1056](/Users/jacobthomas/code/openships/statemachine/supervised/execution.go:1056) increments Revision and sets `Committed` only when the target differs from the current state. Issue an external self-transition, Trip, then Adopt on evidence of completion: the result has `Committed=false` and unchanged Revision. Ordinary Verify of the same row advances Revision. The adjudication documentation promises that Adopt and Override advance Revision.

   Repair direction: decide whether a commit occurred from the adjudication outcome, not from state inequality. Cover adoption of self-transitions and overrides to the current state. A state value can remain equal while a new execution decision commits.

5. **P2 — Lifecycle Record.Change rewrites the original Attempt metadata.**

   [supervised/record.go:186](/Users/jacobthomas/code/openships/statemachine/supervised/record.go:186) reconstructs Change from Result. Verify puts the resulting state Revision and its own operation start time in that Result. Consequently, the Verify callback receives the original Change with revision 0 and Issue-time start, while the emitted Record.Change contains revision 1 and Verify-time start.

   `Record.Revision` already describes the outcome revision. Changing the nested Attempt metadata makes incident correlation with the command actually issued unreliable. Preserve the original Change separately from operation timing and outcome revision, then assert equality between callback Change and corresponding recorded Change.

6. **P2 — Recovery can begin before the prior public Operation finishes.**

   The private Issue implementation releases its operation token at [supervised/execution.go:578](/Users/jacobthomas/code/openships/statemachine/supervised/execution.go:578), before the public wrapper delivers its Recorder callback at line 506. Fail an Issue precondition, hold its cooperative Recorder callback briefly, and call Recover concurrently: the recovery reconciler runs while the prior public Issue and its Recorder callback remain live. Status reports both OperationRunning and CallbackRunning as false.

   This contradicts the full-call lifetime defined in CONTEXT.md, ADR-0002, and LIB-SUP-001. The reproduced overlap is with record delivery, not an unfinished physical Issue action. Repair ownership through public finalization, or explicitly narrow the contract and expose separate delivery health. Coordinate this with the Journal repair so that the lifetime model becomes consistent.

7. **P2 — Trip's documented return-time bound excludes serializer backlog.**

   [supervised/record.go:173](/Users/jacobthomas/code/openships/statemachine/supervised/record.go:173) acquires recordMu before any timeout begins. Concurrent rejected calls accumulate there. Six refused Issue calls with a cooperative Recorder waiting for cancellation made Trip take approximately 147–153 ms with RecorderTimeout set to 20 ms. The documented rough two-interval bound is 40 ms.

   The Fault still latches promptly; it is the public call's return latency that grows with backlog. Bound admission and queue waiting as well as individual deliveries, or describe the backlog-dependent behavior accurately. A separate reproduced reentry case occurs when Recorder calls Trip: nested recording waits on its own caller's recordMu, guaranteeing a Recorder timeout. The timeout eventually breaks the wait, so this is not a permanent deadlock. Support or explicitly constrain such reentry; the Recorder contract currently has no corresponding restriction.

8. **P2 — Bounded record history can discard a newer critical event in favor of an older outcome.**

   [supervised/record.go:159](/Users/jacobthomas/code/openships/statemachine/supervised/record.go:159) evicts by append order, while decisions can arrive for appending out of sequence. With MaxRecords=1, a delayed Issue stamped Seq 1 can append after Trip Seq 2 and evict that newer Trip. Records then returns Seq 1 even though Snapshot.Records is 2. Sorting on read cannot restore an evicted entry.

   The reproduction uses public methods and a nonblocking injected Clock to expose a valid scheduling order. Retain records by causal age or insert them atomically with sequence assignment, while keeping external Recorder delivery outside the state lock. Test retention against the highest committed sequence identities under delayed publication.

9. **P2 — The zero Statechart Instance can panic instead of refusing an event.**

   [statechart/statechart.go:416](/Users/jacobthomas/code/openships/statemachine/statechart/statechart.go:416) hashes the event without the zero-value safeguards present in the flat Machine. This compiles and panics with `hash of unhashable type: []int`:

   ```go
   var i statechart.Instance[any, any, struct{}]
   i.Fire(context.Background(), []int{1}, struct{}{})
   ```

   Compile rejects interface-bearing key types, but the zero Instance never passes through Compile. The zero Instance is explicitly documented as usable. The zero Chart's New path also indexes an unchecked initial state at line 387. Reject invalid dynamic keys or short-circuit an uncompiled Chart before map access; test the exported zero-value paths.

10. **P2 — Key validation accepts values that cannot retrieve their own map entries.**

    [internal/keycheck/keycheck.go:9](/Users/jacobthomas/code/openships/statemachine/internal/keycheck/keycheck.go:9) validates comparability, but float/complex NaN values are comparable without being equal to themselves. This also affects NaNs nested in structs and arrays.

    Reproduced: Compile accepts a NaN event, but Next and Permitted cannot select its row. A flat Instance can successfully commit a NaN destination and then cannot select a declared outgoing NaN row. Statechart compilation accepts duplicate NaN state declarations, yet New cannot load that state. MemoryStore accepts an initial NaN key and subsequently returns ErrNotFound for it.

    Repair direction: validate reflexive equality for actual definition values and accepted Store keys, including destinations; return a clear invalid-key error. Keep the compiled fast path for ordinary typed string/integer keys. Broaden tests beyond integer fuzz inputs and interface comparability checks.

11. **P2, very low natural frequency — Record counters wrap despite an explicit saturation claim.**

    Restore accepts `Snapshot.Records = math.MaxUint64`. Start notices exhaustion, but recording that fault calls unchecked `s.nextRecord++` at [supervised/record.go:151](/Users/jacobthomas/code/openships/statemachine/supervised/record.go:151), producing sequence 0. The comment at [supervised/execution.go:15](/Users/jacobthomas/code/openships/statemachine/supervised/execution.go:15) claims a saturation defense that is absent. Refused operations can also stamp without the normal admission ceiling.

    This is primarily a restore/input-validation defect; reaching the ceiling through ordinary traffic is not realistic. Validate restored counters and make identity stamping itself safe at exhaustion. Define an explicit exhausted outcome that avoids both wraparound and repeated identities. Snapshot fuzzing should execute operations after restoration, not only compare round-trip fields.

12. **P3 — Statechart partial-progress diagnostics count ignored nil actions.**

    [statechart/statechart.go:673](/Users/jacobthomas/code/openships/statemachine/statechart/statechart.go:673) and line 689 increment Completed even when the action is nil. An exit slice `{nil, failingAction}` reports one successfully completed action although none executed successfully. Keep the original ActionIndex, but increment Completed only for nonnil callbacks that return normally. Existing lifecycle tests should assert this metadata explicitly.

**The strongest design decisions deserve preservation.**

- Immutable Machine definitions and separate state owners are a clear domain model. Sharing a definition does not accidentally share an aggregate's state. Compiled rows and lifecycle slices are copied, and executions retain their compiled definition value independently of later whole-value replacement.
- Pure Machine.Next plus effectful Instance/Runtime/Store execution is a meaningful restriction. The small flat implementation is easy to read and efficient. Guard refusal reasons compose naturally through `errors.Is`, and routing/default behavior is documented precisely.
- Instance has a small concurrency model: one in-flight transition, fail-fast overlap, callbacks outside the state lock, and publication after Do succeeds. Cleanup covers panic and Goexit. Caller ownership of mutable data and closure captures is explicit.
- Runtime treats a Run as a root plus FIFO follow-ups, bounds outstanding roots and cumulative work, removes canceled unstarted roots, and handles failures without stranding later roots. Runtime-bound Enqueue catches cross-runtime context mistakes. These semantics earn their implementation complexity.
- The Store seam correctly assigns transaction ownership to the adapter. Optimistic conflicts do not silently replay effects. Step distinguishes attempted work from a Store-confirmed outcome, and the documentation correctly avoids interpreting an ambiguous commit error as rollback proof.
- Statechart keeps Source committed through exit, effect, and entry and only publishes Destination after successful entry. Its inherited selection, initial descent, reentry, and Source/Target LCA behavior are explicit and tested. Position is an effective deep Module: compiled integer relationships and Euler intervals hide hierarchy traversal behind small, allocation-free queries.
- Supervisor makes a valuable distinction between command Issue and fresh Verify evidence. Mandatory checks cannot become routing fallbacks. Stored deadline admission, first-cause faults, operation tokens, callback isolation, and recovery blocking show careful attention to difficult failure modes.
- Observer failures preserve committed state, preserve panic stacks, continue later deliveries, and expose failures through a documented post-commit channel. Optional containment lets callers separate transition errors from observation health.
- The project is candid about partial effects, shallow values, adapter responsibilities, cancellation limits, and the limits of library assurance. CONTEXT.md and the ADRs make terminology and intent unusually easy to recover.
- CI is substantial for this size: three operating systems, race checks, static analysis, vulnerability scanning, coverage retention, live fuzzing, scheduled fuzzing, and release provenance/SBOM steps. Individual actions are pinned to commit hashes.

**Concrete interface and maintenance improvements.**

1. **Let queued callers combine limits with observers.** [queued.NewWithLimits](/Users/jacobthomas/code/openships/statemachine/queued/queued.go:91) supplies nil observers; NewWithObservers always uses default limits. The private constructor already accepts both. Provide one coherent construction route so callers can tune capacity and retain observation delivery. This is an actual missing combination, not a request for more abstract configuration machinery.

2. **Preserve the original queued callback panic stack.** [queued/queued.go:449](/Users/jacobthomas/code/openships/statemachine/queued/queued.go:449) captures only a panic value before replaying it on the caller's goroutine. A caller's recovered stack no longer contains the failing effect. Preserve diagnostic evidence while retaining the documented panic-value identity. Observer and Supervisor failures already provide stronger evidence.

3. **Make observer ergonomics consistent.** Statechart has a distinct typed Observation carrying Info, so the root TimeoutObserver and ContainedObserver helpers cannot be directly applied to it. Offer an appropriate adapter or equivalent typed behavior. Also document that timeout abandonment can overlap later deliveries even for a single execution, and that the wrapper currently passes the original context rather than a timeout-derived context. Cooperative observers should have a clear cancellation path. A blocked ContainedObserver report also remains unbounded unless the outer delivery is bounded.

4. **Strengthen Store conformance checks and evidence.** A deliberately broken FuncStore can swallow a transition error and make Step return `err=nil`, `Confirmed=true`, and a nonnil TransitionError. This violates the existing Store contract; it is not a failure with a correct adapter. Detecting contradictory results would improve diagnostics. A reusable conformance suite should cover callback errors/panics, conditional-write conflicts, cancellation, state/outbox atomicity, and ambiguous commits. The SQL example is a credible sketch, but it is never run against a database. Decide whether production adapters are application-owned; zero dependencies need not be abandoned to offer a conformance suite or optional integration tests.

5. **Clarify lifecycle record semantics.** Retain the explicit adjudication outcome in audit history rather than requiring consumers to infer retain/adopt/override from text and state changes. Distinguish callback identity, operation timing, and resulting state version throughout Result, Record, and Snapshot documentation.

6. **Keep claims synchronized with behavior and releases.** Fix the Unreleased entry that already shipped in v1.3.2. Clarify the README's claim that affordances and handlers “cannot disagree”: the selection rule agrees for the same state and guard-relevant data; those can change between rendering and execution. SECURITY.md promises a signed remediation release, while the implemented workflow creates annotated unsigned tags; specify whether the intended promise is signed tags or verifiable artifact attestations. Failed Journal closure does not always mean the next restoration is in doubt: a failed Trip closure can leave an earlier clean snapshot. Document the actual possible outcomes and required reconciliation.

**Architecture work with the greatest payoff.** These candidates preserve ADR-0001's separate execution models and ADR-0004's pure Machine selection. They describe responsibilities to consolidate, not a proposed new public interface.

1. **Deepen the Supervisor Operation Module.** Files: `supervised/execution.go`, `record.go`, and `result.go`. The current implementation splits state decisions, operation ownership, record stamping, adapter delivery, and durable closure across private operations and public wrappers. Callers must understand more lifetime distinctions than the domain promises. Concentrate finalization and adapter ownership behind the existing Supervisor interface. This gives locality to ordering fixes and lets public-interface tests exercise the full protocol. Dividing the 1,672-line execution file by itself would only move the complexity.

2. **Deepen compiled Statechart hierarchy calculations.** Files: `statechart/statechart.go` and `position.go`. Position already has integer-indexed parent and interval information, while execution rebuilds ancestor slices and an LCA map and repeatedly resolves initial chains. Consolidate that knowledge in the compiled Chart implementation. This improves locality and gives both execution and inspection the same source of hierarchy facts. Avoid eagerly caching every state/transition pair unless measurement justifies the memory cost.

3. **Consolidate shared observation delivery mechanics.** Files: root `observe.go`, `queued/observe.go`, `statechart/observe.go`, and `internal/observer`. The containment primitive is shared, but copying observers, constructing failures, and delivering ordered batches are repeated. Centralize only common delivery mechanics while preserving typed observations and each state owner's sequencing/Run semantics. The payoff is consistent failure behavior and less repeated test setup; a public generic execution engine would add unnecessary interface complexity.

4. **Remove the shallow queued Permitted copy.** [queued/queued.go:245](/Users/jacobthomas/code/openships/statemachine/queued/queued.go:245) rematerializes an already eager immutable Instance.Permitted snapshot. The deletion test is straightforward: removing this second copy removes work without pushing complexity into callers. Delegate to the owned Instance after verifying the eager-snapshot contract remains unchanged.

**Improve the tests' assertions before increasing their volume.**

The test suite is substantial and catches many meaningful failures. Its largest weakness is what some models regard as authoritative.

- Exercise Supervisor public methods when testing persistence, lifecycle records, or Operation lifetime. Many foundational tests call private `start`, `issue`, and `verify`, bypassing the wrappers where the reproduced failures occur.
- Treat controller deliveries, Recorder deliveries, and saved Journal values as independent histories. Crash at every ordering cut, retain all externally observed IDs, and assert that recovery never reuses them.
- Model Journal completion independently of caller timeout. Current fault injection covers returned errors and panics more thoroughly than late writes overtaking subsequent preparations.
- Test recovery and Trip while an earlier public call is still delivering records or closing the Journal. Include multiple rejected calls contending for record delivery.
- Track causal record retention, not just sorted output and the number of evictions.
- Extend successful Snapshot restore tests with Start/Issue/Verify/Adjudicate, particularly at counter limits and with deliberately inconsistent metadata.
- Broaden generic-key tests to zero executions, interface-backed unhashable values, floats, complex numbers, and arrays/structs containing NaNs.
- Add Statechart properties that differ structurally from the implementation: apply observation deltas to old Position membership and compare with the final Position; failure must preserve Position and emit no observations; declaration-order changes must preserve intended behavior. The current reference model repeats much of the production LCA/path algorithm, and generated lifecycle actions predominantly succeed.
- Add a small downstream compile fixture or exported-interface comparison against the previous released tag. Unit tests against current source cannot detect a broken patch upgrade.

**Performance is good at the core; richer execution has measurable costs.**

Local figures below are from one 100 ms run on Apple M1 Pro with Go 1.26.0. Some other checks ran concurrently, so timing is illustrative rather than a regression baseline. Allocation counts are the more stable signal. The Instance, Runtime, and Statechart benchmarks each execute two Fire calls per iteration; the table preserves that unit rather than mislabeling it as one transition.

| Work per benchmark iteration | Time | Allocations | Bytes |
|---|---:|---:|---:|
| Machine.Next accepted | 29.7 ns | 0 | 0 |
| Machine.Permitted iteration | 41.9 ns | 0 | 0 |
| Machine.Next refused | 53.1 ns | 2 | 80 |
| Two Instance fires | 136 ns | 0 | 0 |
| Two observed Instance fires | 2.82 µs | 16 | 1,153 |
| Two Runtime fires | 3.64 µs | 26 | 1,280 |
| Two observed Runtime fires | 6.12 µs | 48 | 2,641 |
| Two Statechart fires | 3.13 µs | 16 | 608 |
| Two observed Statechart fires | 14.6 µs | 56 | 3,809 |
| One Supervisor logical Issue | 8.11 µs | 29 | 3,226 |
| One Supervisor external Issue + Verify | 10.8 µs | 59 | 6,781 |

Supervisor benchmarks explicitly use volatile mode, with no durable Journal or Recorder. They do not measure production storage latency. Observer isolation creates goroutines and channels to contain Goexit; that cost buys behavior and should not be removed casually. Profile realistic observer counts, long Runs, contention, hierarchy depth, and slow storage before changing the concurrency design. Persist currently has no dedicated benchmarks.

CI's `-benchtime=10x` is useful as a smoke check, but ten iterations and no historical comparison cannot establish performance stability. Retain benchmark output, collect repeated samples, normalize the reported work unit, and compare statistically before setting performance budgets.

**Release and tooling improvements.**

- Keep the language minimum separate from the toolchain used for releases. CI, fuzzing, and releases all pin Go 1.26.0. As of this review, the supported patch releases include Go 1.26.8 and Go 1.27.1; both passed this repository's tests. Use a maintained patch toolchain for normal CI/releases and retain a minimum-version compatibility job if needed. [Go release history](https://go.dev/doc/devel/release) records the intervening security and compiler/runtime fixes.
- Add compatibility-aware version selection before automated publication. An annotated tag, SBOM, and build attestation establish provenance; they do not establish backward compatibility or protocol correctness.
- Consider pinning the shared release workflow to its reviewed commit rather than only an exact version tag, consistent with the hash-pinned actions already used. This narrows the trust placed in the release-policy repository's tag immutability. Verify operational protections before treating this as a security finding; none were inspected here.
- Retain failing fuzz inputs in per-change CI as well as the scheduled campaign. The scheduled job uploads them; the smoke job currently exits without a corresponding retention step. Cache useful nonfailing corpus inputs if repeated rediscovery becomes expensive.
- Strengthen the zero-dependency assertion by inspecting the module graph as well as absence of go.sum. The current repository truly has no third-party runtime dependencies; the current check is only an indirect enforcement mechanism.
- Put workflow-dispatch text into an environment variable rather than interpolating it into shell source. The fuzz duration input is currently placed directly in quoted shell code. This is hardening of a maintainer-triggered workflow, not evidence of an untrusted pull-request exploit.
- Establish one authoritative contributor verification command or script if the copies in CI, CONTRIBUTING, and the shared release policy begin drifting. Keep actual tool versions explicit and reproducible.

**Recommended order of work.** First repair late Journal writes, durable identity allocation, and full Operation ownership; validate them with public-interface crash/concurrency tests. Then repair record fidelity, adjudication, causal retention, and counter exhaustion. Address the patch-version compatibility policy before publishing another incompatible release. Fix the compact key-validation and configuration gaps next. Only then consolidate duplicated hierarchy/observer implementations and optimize measured bottlenecks. A wholesale rewrite or merger of the separate state owners is not justified by this review.
