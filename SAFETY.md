# Safety-adjacent use

This repository is general-purpose software, not a safety-rated controller or a certified safety component. Passing tests, race detection, static analysis, or code review does not establish that a robot or machine is safe around people.

## Intended role

The flat Machine, Instance, queued Runtime, Store-backed execution, Statechart, and supervised Supervisor may coordinate application logic. Hazardous motion must remain bounded by an independent, hazard-analyzed safety layer responsible for functions such as emergency stop, safe torque off, protective stop, guarding, overspeed, collision protection, and human-presence separation.

The `supervised` module exists for safety-adjacent orchestration where callers need mandatory checks, explicit issue and verification, finite time budgets, first-cause Fault latching, and reconciliation before startup or recovery. It does not preempt arbitrary Go code or stop hardware.

## Logical and physical state

A committed state is a software fact. It is not evidence that an actuator moved, stopped, braked, or became safe.

An external transition is split deliberately:

1. `Supervisor.Issue` selects the row, runs mandatory checks, durably prepares an in-doubt Snapshot when a Journal is configured, and calls `Issue` without committing its destination.
2. The application obtains fresh controller and sensor evidence.
3. `Supervisor.Verify` runs the transition's verification, rechecks invariants and postconditions, and only then commits.

`IssueCompleted == false` does not prove that an external system received no partial command. An Issue callback may change hardware before returning an error, panicking, calling `runtime.Goexit`, or exceeding its time budget.

## Timeouts and trips

Every Supervisor has finite operation and verification limits. Expiration cancels the callback context, latches a Fault, and prevents logical commit. Go cannot forcibly terminate a callback that ignores cancellation. Such a callback can continue after the caller receives a timeout; `Status.CallbackRunning` exposes this condition and recovery remains blocked until it stops.

`Supervisor.Trip` prevents later logical commit and cancels the active callback context. It is a supervisory inhibition mechanism, not emergency-stop preemption. An emergency event must not depend on acquiring the Supervisor lock, entering an event queue, or waiting for application callbacks.

## Mandatory checks and Guards

Guards route: an error declines one row and may select a later fallback. Never use a Guard as a safety interlock.

Supervisor Preconditions, transition Preconditions, Invariants, Verify, Postconditions, and Reconcile checks are non-bypassable. A failure latches a Fault. Keep Guards and checks deterministic and free of mutation; supply them with a coherent snapshot of relevant application data.

## Startup, persistence, and recovery

A new or restored Supervisor starts stopped. `Start` runs every Reconciler before accepting an Attempt. Restoration validates the definition ID and declared state, but the application must reconcile that logical Snapshot with controller, brake, sensor, calibration, firmware, and durable state.

Recovery preserves the first Fault until all Reconcile checks pass. It is rejected while the prior Operation call or any callback it started remains live. A successful recovery clears the software latch; it does not erase Lifecycle Records, reset an independent safety controller, or authorize motion.

Snapshots carry a non-reusable Execution ID, the Attempt high-water mark, typed identity and timing for an in-doubt Change, and fault metadata. A restored in-doubt Snapshot is Faulted and requires Recover; the library never replays it. Require a durable Journal before external Issue when reuse of an Attempt after process loss is unacceptable. The Journal prepare occurs before the application Issue callback; controller-side command idempotency and fencing are still required for crash consistency across the software/controller seam.

Use a stable Definition ID tied to the deployed transition definition and application build. Definition changes require an explicit Snapshot migration and renewed validation. Do not silently restore a Snapshot under different behavior.

## Authority, fencing, and composition

Supervisor locking is process-local. It does not prove that this process is the exclusive writer for an actuator, coordinate several Supervisors atomically, or authenticate an operator.

A deployed system must provide stable vessel/aggregate/actuator identity, an external exclusive lease or leader, a durable monotonic fencing token, controller rejection of stale fences, command expiry and idempotency, replay protection, and coherent cross-subsystem evidence. Carry the Supervisor Execution ID and Attempt ID into those protocols for correlation. Never treat acceptance by this library as command authorization or exclusive plant ownership.

## Records and observer failures

Configure a Supervisor Recorder for durable Lifecycle Records. The in-process history includes asynchronous verification expiry, Trip, Recover, and secondary causes; `Status.RecorderError` makes a recorder failure visible. A Recorder is bounded by its configured timeout, but Go still cannot terminate a blocked Recorder goroutine.

Instance, Runtime, and Statechart Observer panic or runtime.Goexit failures are contained, retain their original stack, and are returned as errors matching `statemachine.ErrObserverFailed` after the state change commits. Callers must inspect that error and alert or persist it; the committed state remains authoritative.

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
