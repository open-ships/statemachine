# Quality, security, and trust review — 2026-09-10

Reviewed commit: eaff76e91bc5e472012257f52afd544169437cb2.

The library has a strong foundation: immutable definitions, explicit state ownership, a dependency-free root module, clear commit semantics, adapter conformance tests, extensive failure handling, and unusually thorough operating documentation. The main remaining trust issues are failure combinations and lifecycle reporting. Three correctness defects were reproduced. The existing test suite and vulnerability scans pass.

This review records the original findings at the commit above. The follow-up implementation addresses findings 1–4 and adds the SQLite vulnerability scan; see [fix-validation.md](fix-validation.md). The original reproduction output is preserved as before-fix evidence. P1 means address first because an execution guarantee is violated; P2 means a concrete diagnostic or test-assurance defect. None of these findings establishes a remotely exploitable vulnerability or a bypass of application authorization.

## 1. P1 — Expired Verify permits recovery before publication finishes

Location: [supervised/execution.go](../../supervised/execution.go), lines 878–887; compare the timer-driven expiration path at lines 1412–1431.

When Verify finds that its pending Attempt is already expired, it latches the Fault and unlocks without acquiring an Operation or reserving reporting ownership. Finalization subsequently starts Journal and Recorder delivery. Between the decision and those callbacks, both OperationRunning and ReportingRunning are false, so Recover or Adjudicate can be admitted while Verify is still live. Similar gaps exist between the separate delivery calls.

The asynchronous verificationExpired path reserves reporting ownership at the decision point. The synchronous path misses the same protection. Recovery can clear the Fault and admit subsequent work before the old Verify finishes publishing; a closure that captures its Snapshot after recovery can describe the recovered state. This violates the documented recovery exclusion and makes ordering depend on whether the timer or Verify detects expiry.

The reproduction pauses at the existing private decision/finalization seam to model scheduler preemption, without changing Supervisor internals or violating the Clock contract. Recover returns success and ModeReady before the expired Verify is finalized. This is a deterministic interleaving test, not an end-to-end stress test of the public call. Expired Verify still refuses its logical commit.

Fix: centralize expiration decision and publication ownership in the Supervisor Module. Both timer and synchronous admission paths must reserve ownership under the lock and retain exclusion through the complete delivery pipeline and outstanding callbacks. Add tests covering recovery in the decision-to-delivery and between-delivery gaps, including timeout and busy-sink outcomes.

## 2. P2 — Expired Verify drops original command evidence

Location: [supervised/execution.go](../../supervised/execution.go), lines 878–902.

The same expiry branch starts from baseResultLocked and returns before resultFromChange copies the pending command. A public Verify call therefore returns an empty Result.Change, and its RecordVerify contains an empty Record.Change and Issued=false, even though the original Issue succeeded. Result.IssueCompleted is also false. Event, transition, destination, original revision, and start time are absent from the structured Change.

The top-level Attempt identity and retained Fault still provide some evidence, so this is not complete identity loss. But a Recorder consuming the documented structured fields cannot interpret equivalent expiration outcomes consistently: timer-driven expiry preserves the original Change and Issued=true.

Fix: build the expiration outcome from the pending Change before clearing it, preserve known Issue completion, and keep operation timing separate. Compare the complete command identity and outcome flags across successful Verify, synchronous expiry, asynchronous expiry, and fault recovery. The public-interface reproduction fails deterministically.

## 3. P2 — A later queued Goexit discards earlier observer failures

Location: [queued/queued.go](../../queued/queued.go), lines 451–454, 487, and 539–556.

Reproduction: a root commits state 0 to 1; an Observer panics with a distinct cause; the next queued effect calls runtime.Goexit. Fire correctly returns state 1 and ErrExecutionStopped, but loses ErrObserverFailed, the original observer cause, and its stack.

Observer failures accumulate inside run and are joined only on normal return. Goexit unwinds that function, and execute sends a fresh outcome containing only ErrExecutionStopped. A consumer can therefore miss that an already committed change failed to reach its observation sink. This contradicts the observer reporting contract in queued/doc.go.

Fix: retain accumulated observation failures across the goroutine termination seam and combine them with the terminal error. Add a regression combining a committed observer failure with a later Goexit; testing each failure separately is insufficient. The reproduction uses only public interfaces.

## 4. P2 — The queued fuzz oracle does not establish FIFO or exact effect counts

Location: [queued/fuzz_test.go](../../queued/fuzz_test.go), lines 94–96 and 181–214.

FuzzQueuedRuntimeModel compares final state and errors, but its successful transition function is additive modulo three. Successful permutations of the same queued events produce the same final state. The callback execution counter is not asserted, and the model cursor is overwritten with the implementation cursor after each root.

The suite has deterministic FIFO tests; this finding concerns the additional assurance attributed to the generated model. It can miss ordering defects and some extra or omitted effects.

Fix: compare exact expected and actual event traces and callback counts. Keep oracle state independent; encode any permitted cancellation outcomes explicitly. A useful acceptance check is that deliberately reversing dequeue order or duplicating a callback makes the relevant tests fail.

## Validation and security

Executed locally with Go 1.26.8 on macOS arm64:

| Check | Result |
| --- | --- |
| Full scripts/check.sh | Passed vet, errcheck, staticcheck, actionlint, formatting, dependency graph check, race tests, coverage gate, root govulncheck, and SQLite integration tests |
| Existing coverage calculation | 90.7%, above the configured 90% floor |
| Fresh root race run, count=1, coverpkg=./... | Passed; aggregate coverage 93.3% |
| SQLite tests, fresh count=1 with race detector | Passed |
| SQLite module govulncheck with -test | No vulnerabilities found |
| Live fuzzing | All 20 targets passed, three seconds per target |
| Added defect reproductions | All three failed as expected on the reviewed implementation |

The first full script reused cached test results. The additional root and SQLite runs explicitly bypassed the test-result cache. Cross-package coverage shows shared observer implementation coverage at 97.4%; its default package-local 20.8% number does not include callers' tests and should not be mistaken for an untested implementation.

The configured vulnerability scan covers only the root module. The separate SQLite module contains the third-party dependencies, and its driver is used from tests. Add that module's -test scan to the routine checks; the extra scan performed in this review is not a persistent CI control. Keep the root dependency policy.

The release configuration pins its reusable workflow by commit, checks successful same-repository push CI, and the inspected shared workflow checks the intended commit before publication. It generates checksums, an SBOM, and provenance/SBOM attestations. This review inspected configuration, including shared workflow commit 95d4b3ed5452a99525c240688de146553e57cee0; it did not validate a published artifact's attestations or repository administration settings.

No known vulnerability was reported by the scans. This is evidence about the checked code, toolchain, and vulnerability database at review time, not proof of vulnerability freedom. Callback correctness, command authorization, idempotency, durable fencing, and evidence freshness remain application responsibilities, as already documented.

The live fuzz campaign is a smoke test. It does not replace long campaigns or targeted interleavings. Prioritize tests that compose faults, cancellation, reporting, and recovery over raising a statement-coverage percentage alone.

## Reusability, interface consistency, and documentation

Keep Machine, Instance, Runtime, Store, Statechart, and Supervisor separate. Their different ownership and commit rules are deliberate and documented in the ADRs. Making their signatures look alike would not make their semantics identical.

Three concrete improvement candidates:

1. **Deepen Supervisor expiration handling.** The two expiration paths currently duplicate decisions, command projection, and publication ownership. One internal Module would concentrate these guarantees and their tests, improving locality without changing the public Interface.
2. **Deepen queued outcome accumulation.** Share one outcome across ordinary completion and abnormal callback termination. Callers gain reliable combined diagnostics without learning another failure channel.
3. **Deepen inherited Statechart inspection.** Permitted and Arrows restart ancestor traversal for each distinct inherited event. With one distinct event per level, work becomes quadratic. The retained scaling fixture demonstrates this shape. Collecting ordered candidate groups once would improve locality while preserving selection order and guard invocation semantics. This is a scaling recommendation, not a demonstrated remote denial-of-service vulnerability.

Documentation is already extensive: README selection guidance, package comments, examples, CONTEXT, ADRs, SAFETY, migration guidance, and requirements-to-test mappings. The immediate documentation work should correct and test the guarantees above. A compact failure-recovery table would make the scattered contracts easier for humans and agents to consume: committed state, possible external effect, retry policy, retained cause, and required reconciliation for each outcome.

An executable integration example should show an application-owned command envelope, immutable input snapshot, stable command identity, domain error codes, and separate reporting of command outcome and delivery health. Preserve typed Go data inside the library; transport schemas and authorization belong to the integrating Adapter.

## ai-of-empires integration

The referenced project already depends on statemachine v1.4.1. Its game code uses shared Machine definitions and one Instance per lifecycle while World serializes execution. The existing integration is evidence that this ownership model is usable; there is no need to introduce a universal execution Module to support it. See the inspected [go.mod](https://github.com/jakthom/ai-of-empires/blob/adc7a3d57b65b7bfda2cbde2647f1ee43243b1f7/go.mod) and [machine definitions](https://github.com/jakthom/ai-of-empires/blob/adc7a3d57b65b7bfda2cbde2647f1ee43243b1f7/internal/game/machines.go).

For that integration style, prioritize a small compatibility fixture covering initialization/restoration, affordances, effect failure, committed observer failure, and stable domain errors. Use Store plus an outbox when database state and outgoing work must commit together; use Supervisor when issued external work genuinely needs later evidence and reconciliation. The application should continue owning command authorization and transaction scope.

The external project was inspected for integration context only. Its gameplay behavior and security were not audited or tested in this review.

## Evidence and next actions

Run [reproduce.sh](reproduce.sh) from this review directory or the repository root with bash. It temporarily installs the two retained test files, runs the three regression assertions, and cleans up. The command fails on the reviewed baseline and passes with the follow-up fixes. [reproduction-output.txt](reproduction-output.txt) records the observed failures. [fuzz-output.txt](fuzz-output.txt) retains the live campaign output.

The follow-up fixes address findings 1–4, strengthen regression coverage, and add the nested-module scan. Assurance requirements, changelog, and operating documentation describe the corrected guarantees. Optional Statechart scaling and broader integration examples remain recommendations rather than correctness changes in this patch.
