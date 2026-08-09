# Assurance traceability

This matrix separates behavior proved in this repository from product obligations that require external evidence. A passing library test is not a product safety case.

## Library requirements

| ID | Requirement | Automated evidence |
|---|---|---|
| LIB-SUP-001 | A revoked Operation retains ownership until its call and callbacks finish; Recover cannot overlap it. | `supervised.TestRecoverWaitsForWholeRevokedOperation`, `supervised.TestOperationTimeoutLeavesLateCallbackUncommittedAndBlocksRecovery` |
| LIB-SUP-002 | Verify rejects equality with or passage beyond its stored deadline even when timer dispatch is delayed. | `supervised.TestVerifyEnforcesStoredDeadlineAtAdmission` |
| LIB-SUP-003 | A completed mandatory-check error is preserved when a deadline is also observable. | `supervised.TestInvokePreservesCompletedCallbackErrorAtDeadline` |
| LIB-SUP-004 | Caller deadlines are not attributed to the configured Operation budget. | `supervised.TestCallerDeadlineIsNotSupervisorOperationTimeout` |
| LIB-SUP-005 | Fault values returned through Result and Status cannot mutate the retained first cause. | `supervised.TestFaultResultsNeverExposeSupervisorStorage` |
| LIB-SUP-006 | Snapshot preserves Execution ID, Attempt high-water, typed in-doubt Change identity/timing, and fault state across Restore. | `supervised.TestRestorePreservesAttemptHighWaterAndInDoubtState` |
| LIB-SUP-007 | A required Journal prepares an in-doubt Snapshot before external Issue. | `supervised.TestJournalPreparesInDoubtSnapshotBeforeExternalIssue` |
| LIB-SUP-008 | Post-Verify invariant or postcondition failure refuses commit and latches uncertainty. | `supervised.TestPostVerifyMandatoryFailuresLatchWithoutCommit` |
| LIB-SUP-009 | Lifecycle recording includes asynchronous expiry and exposes Recorder failure. | `supervised.TestLifecycleRecorderPublishesAsynchronousExpiryAndFailures` |
| LIB-SUP-010 | Operation budgets use the injected Clock and preserve configured-timeout attribution without wall-clock sleeps. | `supervised.TestOperationTimeoutUsesInjectedClock` |
| LIB-QUE-001 | A canceled root that has not started is removed and never calls application code. | `queued_test.TestCanceledQueuedRootIsRemovedBeforeItStarts` |
| LIB-QUE-002 | Runtime-bound Enqueue rejects a context from another Runtime. | `queued_test.TestRuntimeBoundEnqueueRejectsWrongRuntime` |
| LIB-QUE-003 | Outstanding roots and cumulative Run work have finite limits. | `queued_test.TestRuntimeResourceLimitsBoundRootsAndRunWork` |
| LIB-PER-001 | Store callback repetition or omission is detected; transition effects run at most once per library call. | `persist_test.TestStoreCallbackContractIsEnforced` |
| LIB-OBS-001 | Committed Observations are timestamped; Observer panic and runtime.Goexit retain a stack, later observers run, and committed state remains visible. | `statemachine_test.TestInstanceObservesCommittedPositionChanges`, `statemachine_test.TestInstanceObserverFailuresAreIsolated`, `statechart_test.TestSuccessfulStatechartObserverFailuresAreReturnedAfterCommit` |
| LIB-DEF-001 | Executions own compiled definition values and are unaffected by caller whole-value overwrite. | `queued_test.TestRuntimeOwnsCompiledMachineValue`, `statechart_test.TestInstanceOwnsCompiledChartValue`, `supervised.TestSupervisorOwnsCompiledMachineValue` |
| LIB-KEY-001 | Interface-bearing state/event types and dynamically uncomparable Store keys return errors rather than panic. | `statemachine_test.TestStrictComparableTypesAndDynamicValuesNeverPanic`, `statechart_test.TestCompileRejectsInterfaceBearingKeyTypesBeforeMapUse`, `supervised.TestCompileRejectsNestedInterfaceKeyBeforeMapUse`, `persist_test.TestMemoryStoreRejectsDynamicallyUncomparableKey` |

Repository verification runs `go test`, `go test -race`, `go vet`, errcheck, staticcheck, formatting, a coverage floor, vulnerability scanning, fuzz seeds, and benchmarks. Releases rerun verification on the signed tag and retain the source artifact, coverage, toolchain, SBOM, checksums, and provenance attestation.

## Integration requirements

| ID | Required product evidence |
|---|---|
| INT-AUTH-001 | One externally enforced writer/lease per actuator, with durable monotonic fencing and controller rejection of stale commands. |
| INT-CMD-001 | Command identity, expiry, idempotency, sequencing, replay protection, authentication, and integrity across process/controller restarts. |
| INT-EVID-001 | Timestamped coherent evidence with source identity, maximum age, calibration/health, plausibility, independence, and integrity validation. |
| INT-SAFE-001 | Independent emergency stop, protective stop, safe torque off/brake, overspeed, guarding, and human-separation functions. |
| INT-TIME-001 | Measured worst-case physical response, including scheduler, GC, network, controller, actuator, and Recorder/Journal behavior. |
| INT-TEST-001 | Hazard-derived SIL/HIL/sea-trial tests, power-loss histories, delayed/duplicate/reordered messages, and physical fault injection. |
| INT-OPS-001 | Operator identity, authority handover, alerts, acknowledgement, loss-of-link, degraded mode, and minimum-risk procedures. |
