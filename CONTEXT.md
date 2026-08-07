# State Machine Execution

This repository separates immutable transition definitions from the mutable executions that use them. The distinction lets one definition serve many independently synchronized aggregates.

## Language

**Machine**:
An immutable compiled table defining flat state transitions.
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

**Verification**:
One mandatory check of fresh application evidence before an issued Attempt may commit.
_Avoid_: Acknowledgement, confirmation

**Fault**:
A first-cause execution failure that prevents a Supervisor from accepting another Attempt until reconciliation succeeds.
_Avoid_: Error, state

## Relationships

- One **Machine** is shared by zero or more **Instances** and **Runtimes**.
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
- One **Attempt** issues at most one selected transition and commits it only after its verification succeeds.
- An issued **Attempt** has exactly one successful **Verification**; a purely logical Attempt has none.
- A **Fault** is not a Machine state and does not claim that an external system reached any physical condition.

## Example dialogue

> **Dev:** "Should each order get its own Machine?"
> **Maintainer:** "No. Share the Machine definition; give each order an Instance, a Runtime, or a Store-backed execution depending on who owns its state."

## Flagged ambiguities

- "state machine" previously meant both the immutable transition definition and a running state owner — resolved: **Machine** is the definition; **Instance** or **Runtime** owns execution state.
- "global current status" may mean one execution's complete Position or a census across many executions. This repository provides the former and Observation deltas for building the latter; it does not own a registry of executions.
- "confirmed" in persist means only that Store.Update returned success; physical completion in supervised execution is a **Verification**.
