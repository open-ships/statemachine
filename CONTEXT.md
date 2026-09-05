# State Machine Execution

This repository separates immutable transition definitions from the mutable executions that use them. The distinction lets one definition serve many independently synchronized aggregates.

## Language

**Machine**:
An immutable compiled table defining flat state transitions. Next runs Guards
and reports a destination but never runs Do; effects require a state owner.
_Avoid_: Instance, runtime

**Instance**:
One in-memory execution of a Machine with a single current state and fail-fast overlap semantics.
_Avoid_: Machine, aggregate

**Run**:
One externally requested event together with the follow-up events it enqueues for run-to-completion execution.
_Avoid_: Transition, transaction

**Runtime**:
A state owner that serializes Runs and drains each Run to completion.
_Avoid_: Machine, worker

**Statechart**:
An immutable hierarchical transition definition with initial substates and lifecycle actions.
_Avoid_: Machine, Instance

**Store**:
A persistence adapter that owns one conditional state update around a transition callback. A transactional Store may expose its transaction to effects.
_Avoid_: Repository, setter

**Observation**:
One committed node exit or entry emitted by an Instance or Runtime. Observations describe position changes, not attempted transitions or action execution.
_Avoid_: Event, transition

**Observer**:
An immutable execution adapter that receives ordered Observations together with the context and data used to perform the change.
_Avoid_: Hook, registry

**Step**:
The non-empty batch of Observations produced by one committed position change. A transition that does not change position has no Step.
_Avoid_: Run, transaction

**Position**:
An immutable projection of one Statechart's hierarchy at one committed active state.
_Avoid_: Instance, registry

**Supervisor**:
One bounded execution of a strict flat definition that separates command issue from verification and latches execution faults.
_Avoid_: Runtime, safety controller

**Attempt**:
One event accepted by a Supervisor for selection, issue, and optional verification under a single identifier.
_Avoid_: Run, Step

**Change**:
A candidate or selected transition paired with its original Attempt identity, source, destination, revision, and start time. Lifecycle operations without a selected transition may carry a zero or operation-specific Change.
_Avoid_: Outcome, committed revision

**Operation**:
One call to Start, Issue, Verify, Recover, or Adjudicate together with every callback it starts, including Journal and Recorder delivery. An Operation remains live until the call has returned and all detached callbacks have stopped.
_Avoid_: Attempt, Run

**Execution ID**:
A durable, non-reusable identity for one Supervisor lineage. Attempt identity is Execution ID, Incarnation ID, and sequence.
_Avoid_: Definition ID, Revision

**Incarnation ID**:
A fresh random identity for one construction or restoration of a Supervisor. It prevents identity reuse when the latest counters were never saved. It does not establish ordering or writer authority.
_Avoid_: Restarts, fence

**Verification**:
One mandatory check of fresh application evidence before an issued Attempt may commit.
_Avoid_: Acknowledgement, confirmation

**Fault**:
A first-cause execution failure that prevents a Supervisor from accepting another Attempt until reconciliation succeeds.
_Avoid_: Error, state

**Lifecycle Record**:
One immutable, sequenced outcome in a Supervisor's execution history. Lifecycle Records describe attempts and faults; Observations describe committed node membership.
_Avoid_: Observation, Fault

**Journal**:
A durable Adapter that saves an in-doubt Snapshot, including the typed Change and its timing, before an external Issue callback can run.
_Avoid_: Store, Recorder

## Relationships

- One **Machine** is shared by zero or more **Instances** and **Runtimes**.
- A **Machine** may answer a pure Next query for caller-owned state, but only an
  **Instance**, **Runtime**, or **Store**-backed execution runs a transition's Do.
- One **Instance** owns exactly one current state.
- One **Runtime** owns exactly one current state and serializes zero or more **Runs**.
- One **Run** contains one root event and zero or more follow-up events.
- A callback appends follow-ups to its current **Run**; it never synchronously starts another Runtime Run.
- A **Statechart** creates independently stateful statechart instances.
- A Statechart guard receives transition information, including the active source and inherited handler.
- A **Store** applies a Machine transition inside one adapter-defined unit of work.
- A Store invokes a loaded transition once and never retries effects automatically.
- An **Observation** belongs to exactly one **Step** and names exactly one exited or entered state.
- An **Observer** is attached immutably for an Instance or Runtime's lifetime and is never attached to a Machine. The same Observer value may be shared by many executions; identity belongs in their typed data or context.
- One committed position change produces zero or one Step. Flat self-transitions and Statechart internal transitions produce none.
- A Runtime Run may contain zero or more Steps. Its Run identifier is the Step identifier of its first non-empty Step.
- A **Position** contains one active state, zero or more enclosing ancestors, and no execution registry.
- One **Supervisor** owns exactly one committed state, zero or one pending Attempt, and zero or one latched Fault.
- One strict Machine definition may be shared by zero or more **Supervisors**.
- One **Attempt** selects at most one transition for execution. Normal execution commits an external transition after verification succeeds; a purely logical transition can commit during Issue. Explicit Adjudicate can instead resolve a Fault through reconciliation.
- One **Operation** owns a non-reusable token. A revoked Operation retains that token until its call and callbacks finish, so stale continuations cannot mutate a later Operation.
- A restored Supervisor preserves its **Execution ID**, saved Attempt high-water mark, and typed in-doubt Change evidence, and generates a fresh **Incarnation ID**. Pending evidence retains its original command's Incarnation ID. An in-doubt or faulted Snapshot restores Faulted and must reconcile through Recover or Adjudicate; it is never replayed automatically.
- An external **Attempt** committed through Verify has one successful **Verification**. An issued Attempt that faults or remains pending may have none, and AdjudicateAdopt uses explicit reconciliation instead. A purely logical Attempt has none.
- A **Fault** is not a Machine state and does not claim that an external system reached any physical condition.
- A configured **Journal** durably records an in-doubt Snapshot before Issue. Controller fencing and atomic plant commands remain external responsibilities.
- Lifecycle Record identity is **Execution ID**, **Incarnation ID**, and Seq. Seq orders decisions within an incarnation; Restarts is a saved restoration count, not a uniqueness authority.
- Recover does not reset Supervisor lifecycle history; ordinary bounded retention still applies. History retains the highest Seq values, regardless of publication order. A Recorder failure or busy delivery is exposed through Status.
- Within one Supervisor, each Journal or Recorder admits at most one outstanding delivery and retains ownership until it ends. A shared adapter can still be called concurrently by different Supervisors. Trip and verification-expiry publication also prevent recovery until their delivery pipeline finishes.
- For a selected Attempt and its Verify or Adopt outcome, **Change** retains the original Attempt revision and start time. A Result or Record separately reports the resulting revision, adjudication outcome, and operation timing.

## Example dialogue

> **Dev:** "Should each order get its own Machine?"
> **Maintainer:** "No. Share the Machine definition; give each order an Instance, a Runtime, or a Store-backed execution depending on who owns its state."

## Flagged ambiguities

- "state machine" previously meant both the immutable transition definition and a running state owner — resolved: **Machine** is the definition; **Instance** or **Runtime** owns execution state.
- "global current status" may mean one execution's complete Position or a census across many executions. This repository provides the former and Observation deltas for building the latter; it does not own a registry of executions.
- "confirmed" in persist means Store.Update returned success and passed the detectable contract checks; it does not independently prove persistence. Physical completion in supervised execution requires application evidence checked by **Verification**.
