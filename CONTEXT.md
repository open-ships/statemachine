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

## Example dialogue

> **Dev:** "Should each order get its own Machine?"
> **Maintainer:** "No. Share the Machine definition; give each order an Instance, a Runtime, or a Store-backed execution depending on who owns its state."

## Flagged ambiguities

- "state machine" previously meant both the immutable transition definition and a running state owner — resolved: **Machine** is the definition; **Instance** or **Runtime** owns execution state.
