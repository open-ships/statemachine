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
//  4. Issue runs;
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
// Operation and verification limits are mandatory. A timeout cancels the
// callback context, latches a Fault, and prevents logical commit. Go cannot
// forcibly terminate arbitrary callback code: a callback that ignores its
// context may continue mutating external systems. Status reports this as
// CallbackRunning, and Recover is rejected until it stops.
//
// This package is not a safety-rated controller and must not be the sole path
// for emergency stop, safe torque off, guarding, collision protection, or
// human-presence separation. Those functions require an independent,
// hazard-analyzed safety layer. See the repository's SAFETY.md.
package supervised
