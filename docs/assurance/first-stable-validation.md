# First stable validation record

Validated on 2026-09-05 against the local 1.4.0 working tree based on commit `9538650`, on macOS arm64, Apple M1 Pro. This records release preparation; no tag or release was published during this work. Historical review evidence remains in `scratch/review-2026-09-05` and refers to the original implementation, not this corrected working tree.

## Review outcomes

| Finding | Implemented behavior |
|---|---|
| Late Journal writes can overtake later preparation | The Journal retains its exclusive delivery lease until the callback ends, including after a caller timeout. Admission/recovery observe that ownership. |
| Restarts and accepted refusals can reuse identities | Fresh random IncarnationID participates in Attempt and Record identity on every restore. Startup and accepted refusal closures save counters when delivery succeeds. |
| A historical patch removes Machine.Fire | 1.4.0 is the first stable baseline; earlier tags remain unchanged prerelease history. Pure Next stays, with explicit effect-preserving migration guidance. |
| Self-transition adjudication omits a revision | Adopt and Override commit and increment Revision even when the state value stays equal. Retain does not. |
| Record.Change rewrites command metadata | Original Change revision, timing, and incarnation survive Issue, Verify, in-doubt restoration, and adoption. Outcome metadata is separate. |
| Public operations release ownership too early | Shared finalization covers Journal and Recorder; detached callbacks and late secondary reporting remain owned until completion. |
| Trip waits behind an unbounded Recorder queue | Each sink has one fail-fast gate. Busy delivery is visible, records remain in memory, and Recorder reentry can Trip without waiting on itself. |
| Late publication evicts newer causal records | Bounded retention keeps the highest Seq values, in Seq order. |
| Zero Statechart panics on invalid dynamic keys | Invalid keys return errors before map access. |
| NaN keys cannot retrieve their own entries | Reflexive key validation covers scalar and nested float/complex keys; common string/integer paths remain fast. |
| Record sequences wrap | Restore/admission refuse exhausted counters; final diagnostic exhaustion drops records without reusing Seq. |
| Nil lifecycle actions inflate completed counts | ActionError.Completed counts only callbacks that actually returned successfully. |

Additional improvements include combined queued options, originating panic evidence, shared typed observer delivery and Statechart helpers, derived observer deadline contexts, a shared compiled Statechart hierarchy, stronger Store contract checks, a reusable adapter suite, a separate real SQLite integration module, and normalized/scaling benchmarks. Independent implementation reviews also identified and fixed arbitrary application error formatting under Supervisor ownership and locks.

[Requirements](requirements.md) map these contracts to tests. [Migration](../../MIGRATION.md) documents the new identities, snapshot schema 2, delivery behavior, and first stable policy.

## Executed checks

- `GOTOOLCHAIN=go1.26.8 ./scripts/check.sh`: passed vet, pinned errcheck/staticcheck, workflow lint, formatting, the dependency-free root-module graph check, race tests, coverage gate, vulnerability scanning, and SQLite race integration. Final aggregate statement coverage: **90.7%**, required minimum **90%**. Root-module govulncheck reported no vulnerabilities.
- `GOTOOLCHAIN=go1.26.0 ./scripts/check.sh test`: passed minimum-toolchain vet and tests.
- `GOTOOLCHAIN=go1.27.1 ./scripts/check.sh test`: passed current-toolchain vet and tests.
- `GOTOOLCHAIN=go1.26.8 FUZZTIME=3s ./scripts/fuzz.sh`: all **20 targets passed**, three seconds per target, in addition to focused live fuzzing during implementation. This is a smoke campaign, not exhaustive exploration.
- Repeated Supervisor regression race tests passed. The callback-at-deadline test now arranges publication before deadline observation, avoiding an invalid assumption about a callback that closes Done before returning.
- `GOTOOLCHAIN=go1.26.8 ./scripts/benchmark.sh`: all benchmarks passed with three 100 ms samples and allocation reporting.
- `git diff --check`: passed.

Selected benchmark ranges from that local run:

| Workload | Time per operation | Allocations |
|---|---:|---:|
| Machine.Next accepted | 26.44–26.75 ns | 0 |
| Instance.Fire, one call | 62.62–67.94 ns | 0 |
| Machine.Permitted | 40.48–41.14 ns | 0 |
| Statechart.Fire, one call | 218.7–221.2 ns | 1, 64 B |

The reviewed Statechart baseline used approximately 304 B and eight allocations per Fire. Timing depends on hardware, scheduling, and toolchain; it is not a hosted-CI performance guarantee. Generated `coverage.out` and `benchmark.out` are ignored local artifacts; CI is configured to retain equivalent evidence.

## Scope limits

Linux and Windows jobs are configured but were not executed locally. GitHub-hosted CI, tagging, artifact signing/attestations, and publication were not triggered. The root module retains zero third-party dependencies; SQLite test dependencies are isolated in its nested module.

Adapter timeouts cannot terminate Go code. Busy delivery is not automatically retried. A failed closure can leave a clean or in-doubt saved Snapshot, and random incarnation identities do not prove freshness or exclusive writer authority. SQLite tests establish local transaction behavior; the lost-response case injects a response failure after a real commit and does not reproduce remote-network commit ambiguity. Application reconciliation and production-database validation remain necessary as documented in [known limitations](known-limitations.md).
