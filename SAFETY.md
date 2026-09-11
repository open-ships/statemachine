# Safety-adjacent use

This repository is general-purpose software, not a safety-rated controller or a certified safety component. Passing tests, race detection, static analysis, or code review does not establish that a robot or machine is safe around people.

## Intended role

The flat Machine, Instance, queued Runtime, Store-backed execution, Statechart, and supervised Supervisor may coordinate application logic. Hazardous motion must remain bounded by an independent, hazard-analyzed safety layer responsible for functions such as emergency stop, safe torque off, protective stop, guarding, overspeed, collision protection, and human-presence separation.

`Machine.Next` is selection-only: it runs Guards and reports a destination but
never runs a transition's `Do`. Flat effects are available only through an
Instance, queued Runtime, or Store-backed execution that owns the associated
state commit. This prevents an ignored Machine result from running `Do`;
Guards must remain pure. It does not make arbitrary external I/O atomic or reversible.

The `supervised` module exists for safety-adjacent orchestration where callers need mandatory checks, explicit issue and verification, finite time budgets, first-cause Fault latching, and reconciliation before startup or recovery. It does not preempt arbitrary Go code or stop hardware.

## Logical and physical state

A committed state is a software fact. It is not evidence that an actuator moved, stopped, braked, or became safe.

An external transition is split deliberately:

1. `Supervisor.Issue` selects the row, runs mandatory checks, durably prepares an in-doubt Snapshot when a Journal is configured, and calls `Issue` without committing its destination.
2. The application obtains fresh controller and sensor evidence.
3. `Supervisor.Verify` runs the transition's verification, rechecks invariants and postconditions, and only then commits.

`IssueCompleted == false` does not prove that an external system received no partial command. An Issue callback may change hardware before returning an error, panicking, calling `runtime.Goexit`, or exceeding its time budget.

## Timeouts and trips

Every Supervisor has finite operation and verification limits. Expiration during pending execution cancels its callback context, latches a Fault, and prevents that operation's logical commit. Journal and Recorder delivery timeouts have separate semantics: they expose delivery failure and cannot undo an already committed outcome. Go cannot forcibly terminate a callback that ignores cancellation. Such a callback can continue after the caller receives a timeout; `Status.CallbackRunning`, `JournalRunning`, and `RecorderRunning` expose live callbacks, and recovery remains blocked until their ownership ends. Verification expiry reserves its complete publication lifetime whether a timer or Verify detects the deadline, and both paths preserve the original Change and known Issue completion. Recovery and adjudication remain excluded between the decision and delivery callbacks as well as while callbacks run.

`Supervisor.Trip` prevents the active operation's later logical commit and cancels its callback context. It is a supervisory inhibition mechanism, not emergency-stop preemption. The fault latches before Trip returns, but the return itself includes bounded record and journal-closure delivery. Within one Supervisor, each adapter admits one delivery at a time; a busy adapter fails fast, and its prior callback retains ownership until it ends. An emergency event must not depend on acquiring the Supervisor lock, entering an event queue, waiting for application callbacks, or Trip's return latency.

## Mandatory checks and Guards

Guards route: an error declines one row and may select a later fallback. Never use a Guard as a safety interlock.

Supervisor Preconditions, transition Preconditions, Invariants, Verify, Postconditions, and Reconcile checks are non-bypassable. A failure latches a Fault. Keep Guards and checks deterministic and free of mutation; supply them with a coherent snapshot of relevant application data.

## Startup, persistence, and recovery

A new Supervisor or a clean restoration starts stopped. An in-doubt or faulted restoration starts Faulted and requires Recover or Adjudicate. `Start` runs every Reconciler before a stopped Supervisor accepts an Attempt. Restoration validates the definition ID and declared state, but the application must reconcile that logical Snapshot with controller, brake, sensor, calibration, firmware, and durable state.

Recovery preserves the first Fault until all Reconcile checks pass. It is rejected while the prior Operation call or any callback it started remains live. A successful recovery clears the software latch without resetting lifecycle history; ordinary bounded retention still applies. Recovery does not reset an independent safety controller or authorize motion.

`Supervisor.Adjudicate` resolves a Fault with an explicit Decision: retain the committed state, adopt the in-doubt Change's destination when the application proves the controller completed it, or override to a declared minimum-risk state. Every Reconciler validates the proposed state before the Supervisor returns to Ready, and the Decision's Evidence is retained verbatim in the lifecycle Record. Adopt and Override commit a revision even when state does not change; adoption does not claim that Verify succeeded. Durability depends on successful Journal/Recorder delivery. Adjudication records the decision; establishing that the evidence is true remains the application's obligation.

Snapshots carry schema version 2, an Execution ID for the lineage, the current random Incarnation ID, a saved Restarts count, Attempt and Record high-water marks, the original in-doubt Change and its timing, and fault metadata. Attempt identity is (ExecutionID, IncarnationID, Sequence); Record identity is (ExecutionID, IncarnationID, Seq). Construction and restoration generate fresh incarnation identities. Pending.Attempt and Fault.Change retain the original external command's incarnation, which may differ from the restored Supervisor's current incarnation. Random incarnation identity prevents reuse after unsaved counter updates, but is not a monotonic fence or proof of exclusive ownership.

An in-doubt Snapshot restores Faulted and requires Recover or Adjudicate; the library never replays it. Machines with external Issue actions require a durable Journal by default; Options.Unjournaled is the explicit waiver for tests and non-hazardous work. Successful preparation occurs before application Issue. Closure delivery follows startup, accepted refusals, commits, fresh faults, and recovery. A failed or skipped closure may leave a clean or in-doubt Snapshot: inspect delivery health and reconcile on every startup. Old writes cannot overlap a later preparation within the same Supervisor, but external writer authority and controller idempotency remain application responsibilities. Snapshot validation cannot detect rollback; compare its counters against an independent durable authority. Do not use random IncarnationID as that authority.

Use a stable Definition ID tied to the deployed transition definition and application build. Definition changes require an explicit Snapshot migration and renewed validation. Do not silently restore a Snapshot under different behavior.

## Authority, fencing, and composition

Supervisor locking is process-local. It does not prove that this process is the exclusive writer for an actuator, coordinate several Supervisors atomically, or authenticate an operator.

A deployed system must provide stable vessel/aggregate/actuator identity, an external exclusive lease or leader, a durable monotonic fencing token, controller rejection of stale fences, command expiry and idempotency, replay protection, and coherent cross-subsystem evidence. Carry the Supervisor Execution ID and Attempt ID into those protocols for correlation. Never treat acceptance by this library as command authorization or exclusive plant ownership.

## Records and observer failures

Configure a Supervisor Recorder for durable Lifecycle Records. Record identity (ExecutionID, IncarnationID, Seq) is assigned atomically with the state decision it describes, so Seq order is causal order and restored executions never reuse identity; wall-clock timestamps are correlation evidence only. The in-process history includes asynchronous verification expiry, Trip, Recover, Adjudicate, journal-closure failures, and secondary causes, and is bounded by `Limits.MaxRecords` with evictions counted in `Status.RecordsDropped`. `Status.RecorderError`, `Status.RecorderFailures`, and `Status.JournalError` make delivery failures visible. Recorder and Journal timeouts bound waiting; their callbacks retain ownership until they end. At most one abandoned callback per sink can remain blocked in a Supervisor. Busy delivery is recorded as a failure without a queue or automatic retry. History keeps the highest Seq values despite reordered publication; exhausted counters stop recording instead of wrapping.

Instance, Runtime, and Statechart Observer panic or runtime.Goexit failures are contained, retain their original stack, and are returned as errors matching `statemachine.ErrObserverFailed` after the state change commits. Callers must inspect that error and alert or persist it; the committed state remains authoritative. If a later queued callback calls runtime.Goexit, Fire joins ErrExecutionStopped with the earlier committed Steps' observer failures, including their causes and stacks.

Queued Runtime uses finite root and cumulative Run limits. Context cancellation can remove a root that has not started. These resource controls bound library work, not callback memory, controller queues, or external work launched by callbacks.

## Assurance expectations

A human-adjacent system still needs, at minimum:

- an application-specific hazard analysis and required safety performance determination;
- requirements-to-test traceability and independent review;
- physical fault injection for sensors, communications, power, brakes, controllers, and actuators;
- watchdogs and stale-input detection outside the application process;
- deterministic resource limits for the deployed platform;
- secure command authorization, update control, and definition provenance;
- a durable recorder correlated with safety-controller, sensor, and actuator evidence; and
- validation under the standards and regulations applicable to the robot class and jurisdiction.

Treat every application callback and Adapter as part of the system safety case. This library cannot validate the correctness, freshness, independence, or integrity of the evidence supplied to it.
