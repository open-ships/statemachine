# Follow-up fix validation — 2026-09-10

The follow-up patch addresses all four numbered findings in [review.md](review.md), and adds the missing SQLite test-dependency vulnerability scan. These are compatible behavior fixes; no public method signatures or snapshot schema change.

## Corrected behavior

- Synchronous Verify and timer-driven expiry share one fault-decision implementation. Reporting ownership is reserved under the Supervisor lock before publication and released by shared finalization. Expiry may borrow an enclosing Issue Operation for callback accounting but cannot mark that public call as returned.
- Expiration results and records retain the pending Change and known Issue completion. The expiry decision time is separate from the original command's start time.
- Queued execution shares its outcome across normal return and goroutine termination. Deferred aggregation retains observer errors, causes, and origin stacks from earlier committed Steps when a later callback calls runtime.Goexit.
- The queued fuzz oracle compares exact execution order, effect count, independent instruction cursor, committed state, and synchronous cancellation outcomes.
- The full source-check script scans the separate SQLite module with govulncheck -test, using the same pinned or CI-supplied tool selection as the root scan.

Permanent regressions are in [expiry_publication_test.go](../../supervised/expiry_publication_test.go) and [observer_failure_test.go](../../queued/observer_failure_test.go). They cover pre-delivery recovery/adjudication exclusion, original command metadata, enclosing Issue ownership, detached Journal/Recorder callbacks, and observer cause/stack retention after Goexit.

## Executed validation

Local platform: macOS arm64, Apple M1 Pro.

| Command or check | Result |
| --- | --- |
| GOTOOLCHAIN=go1.26.8 ./scripts/check.sh | Passed vet, errcheck, staticcheck, actionlint, formatting, root dependency policy, race tests, coverage gate, SQLite integration, and both vulnerability scans |
| Configured statement coverage | 90.8%, minimum 90% |
| GOTOOLCHAIN=go1.26.0 ./scripts/check.sh test | Passed |
| GOTOOLCHAIN=go1.27.1 ./scripts/check.sh test | Passed |
| Root queued/supervised tests with -race -count=1 | Passed |
| Independent implementation review | No actionable regression found; targeted regressions also passed with -race -count=20 |
| All 20 fuzz targets, three seconds each | Passed |
| Queued model with -race and 20 seconds of live fuzzing | Passed, 453,734 executions |
| Original reproduction script | All three formerly failing assertions now pass; see [fixed-reproduction-output.txt](fixed-reproduction-output.txt) |
| Benchmark script, three samples per benchmark | Passed; concurrent validation workloads mean timings are not a regression comparison |
| git diff --check | Passed |

An independent reviewer also ran ten seconds of the queued model without failures.

Two deliberately faulty builds were checked with temporary Go build overlays, leaving production files intact. Reversing dequeue order failed the new exact-trace assertion on the FIFO seed. Duplicating execution also failed that seed. The isolated overlays were discarded from the patch; [mutation-lifo-output.txt](mutation-lifo-output.txt) and [mutation-duplicate-output.txt](mutation-duplicate-output.txt) retain the observed failures.

No known vulnerabilities were found by the root or SQLite test-dependency scans. This does not establish vulnerability freedom. Hosted Linux/Windows checks and release artifact verification were not performed locally. The optional Statechart scaling work and broader integration examples remain separate recommendations.
