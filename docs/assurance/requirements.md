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
| LIB-SUP-011 | Lifecycle Record identity (ExecutionID, Restarts, Seq) is assigned atomically with the state decision, is strictly increasing, and is never reused across restorations. | `supervised.FuzzSupervisorModel`, `supervised.FuzzConcurrentSupervisor`, `supervised.TestRestoreContinuesRecordIdentityAcrossIncarnations` |
| LIB-SUP-012 | In-process Record history is bounded, evictions are counted, and Recorder failures are counted and visible without rewriting outcomes. | `supervised.TestRecordsRingBoundsHistoryAndCountsDrops`, `supervised.FuzzRecorderJournalFaultInjection` |
| LIB-SUP-013 | The Journal receives an in-doubt Snapshot before external Issue and a closure Snapshot after every commit, fresh Fault, and completed recovery, in decision order; closure failure is visible and leaves restoration conservative. | `supervised.TestJournalReceivesPrepareAndClosureInDecisionOrder`, `supervised.TestJournalClosureFailureIsVisibleWithoutRewritingOutcome`, `supervised.FuzzSupervisorRestartModel` |
| LIB-SUP-014 | Machines with external Issue actions cannot be constructed without a Journal unless the volatile mode is explicitly named. | `supervised.TestConstructionRequiresJournalForExternalMachines` |
| LIB-SUP-015 | Adjudication commits only decisions applicable to the latched Fault, validates the proposed state through every Reconciler, and durably records the outcome with verbatim Evidence. | `supervised.TestAdjudicateAdoptCommitsInDoubtDestination`, `supervised.TestAdjudicateRejectsInapplicableDecisions`, `supervised.TestAdjudicateReconcilersReceiveProposedState`, `supervised.FuzzSupervisorRestartModel` |
| LIB-SUP-016 | Restore validates the Snapshot schema version and refuses structurally inconsistent Snapshots with documented sentinels, never a panic. | `supervised.TestRestoreValidatesSnapshotSchemaVersion`, `supervised.FuzzSnapshotRestore` |
| LIB-SUP-017 | A delivered callback outcome is the primary cause of its operation's decision; secondary-cause Records exist only for outcomes abandoned at a deadline. | `supervised.TestSecondaryCauseIsRecordedOnlyForAbandonedCallbacks` |
| LIB-OBS-002 | Observer delivery can be time-bounded and observation health can be separated from the transition error channel without un-committing state. | `statemachine_test.TestTimeoutObserverBoundsBlockedDelivery`, `statemachine_test.TestContainedObserverSeparatesObservationHealth`, `statemachine_test.TestContainedTimeoutCompositionKeepsFireClean` |
| LIB-FUZ-001 | Model-based fuzzing checks every package against independent reference implementations: supervisor lifecycle and power-loss restart, adversarial clock, concurrent schedules, recorder/journal fault injection, definition and snapshot fuzzing, flat-machine and statechart selection/path semantics, queued cascade semantics, and Store contract violations. | `FuzzSupervisorModel`, `FuzzSupervisorRestartModel`, `FuzzAdversarialClock`, `FuzzConcurrentSupervisor`, `FuzzSnapshotRestore`, `FuzzDefinitionCompile`, `FuzzRecorderJournalFaultInjection`, `FuzzMachineFire`, `FuzzZeroAndCompiledKeyChecks`, `FuzzStatechartCompile`, `FuzzStatechartFire`, `FuzzStoreContract`, `FuzzMemoryStoreSerialization`, `FuzzQueuedRuntimeModel`, `FuzzQueuedRuntimeLimits`, `FuzzQueuedCancellation` |

Repository verification runs `go test`, `go test -race`, `go vet`, errcheck, staticcheck, formatting, a coverage floor, vulnerability scanning, per-change fuzz smoke runs of every target, benchmarks, and a scheduled sustained fuzz campaign. Failing fuzz inputs are retained under `testdata/fuzz` as permanent regression seeds. After successful CI on the current `main` commit, the exact-version-tagged shared release workflow reruns source verification, creates an annotated tag automatically, and retains the source artifact, coverage, toolchain, SBOM, checksums, and separate build-provenance and SBOM attestations.

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
