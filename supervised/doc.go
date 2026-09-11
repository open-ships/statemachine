// Package supervised provides strict, bounded, fault-latching execution for a
// flat state-machine definition.
//
// A Machine is immutable and shared. A Supervisor owns one committed state and
// must be started through reconciliation before it accepts an Attempt. Each
// Attempt follows one fixed order:
//
//  1. every global Precondition passes;
//  2. Guards select the first applicable transition;
//  3. every transition Precondition and global Invariant passes;
//  4. an optional durable Journal prepares an in-doubt Snapshot, then Issue runs;
//  5. a later Verify call establishes application-defined physical completion;
//  6. Invariants and Postconditions pass; and
//  7. the logical destination commits and Revision increments.
//
// Preconditions, Invariants, verification, and Postconditions are mandatory
// checks. Their errors latch the first Fault and never route to another row.
// Guard errors are different: they decline only that row, preserving ordinary
// guarded fallback semantics.
//
// A transition with nil Issue and Verify is purely logical and commits during
// Supervisor.Issue. Otherwise both callbacks are required. Issue returning nil
// means only that its callback completed; it does not prove that hardware
// received, completed, or safely stopped a command. Verify is the application
// seam for fresh controller and sensor evidence.
//
// Every public operation returns both its detailed Result and an ordinary error
// so errcheck-class tools can detect discarded failures. Attempt identity is
// (ExecutionID, IncarnationID, Sequence); every restore has a fresh incarnation.
//
// Operation and verification limits are mandatory. A timeout cancels the
// callback context, latches a Fault, and prevents logical commit. Go cannot
// forcibly terminate arbitrary callback code: a callback that ignores its
// context may continue mutating external systems. Status reports this as
// CallbackRunning, and Recover is rejected until the prior Operation call and
// every callback it started have stopped. Verification admission checks its
// stored monotonic deadline synchronously; the timer is notification only.
// Both expiry paths retain the original Change and known Issue completion,
// expose ReportingRunning through publication, and exclude recovery until
// publication and any detached sink callbacks finish.
//
// NewWithOptions supplies deterministic Clock, durable Journal, and Recorder
// seams. A Machine that declares external Issue actions requires a Journal by
// default; Options.Unjournaled is the explicitly named waiver for tests and
// non-hazardous work. The Journal receives an in-doubt Snapshot before Issue
// and attempts a closure after startup, accepted refusals, commits, fresh
// faults, and recovery. A failed closure can leave a clean or in-doubt saved
// Snapshot; application reconciliation is always required. Adapter ownership
// lasts until the actual callback ends, even after a caller timeout. A busy
// sink fails delivery immediately and no automatic retry occurs.
//
// Lifecycle Records are identified by (ExecutionID, IncarnationID, Seq), assigned
// atomically with the state decision each describes, bounded in memory by
// Limits.MaxRecords, and retained across Recover. Recorder and Journal
// failures are visible in Status and never rewrite a completed outcome.
//
// A latched Fault is resolved by Recover, which validates and retains the
// committed state, or by Adjudicate, which records an explicit Decision to
// retain the state, adopt the in-doubt Change's destination on controller
// evidence, or override to a declared minimum-risk state.
//
// This package is not a safety-rated controller and must not be the sole path
// for emergency stop, safe torque off, guarding, collision protection, or
// human-presence separation. Those functions require an independent,
// hazard-analyzed safety layer. Safety scope and integration obligations:
// https://github.com/open-ships/statemachine/blob/main/SAFETY.md
package supervised
