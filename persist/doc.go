// Package persist applies a flat statemachine Machine through an adapter-owned
// unit of work.
//
// A Store is deliberately stronger than a getter and setter. Its Update method
// loads one aggregate state, invokes a transition callback exactly once, and
// conditionally commits the returned state in the same unit of work. An SQL
// adapter can pass its transaction to the callback, allowing the transition's
// effect to update the aggregate and append an outbox record atomically with
// the state change. Only work performed through that unit is atomic; a network
// call made by an effect cannot be rolled back by the Store.
//
// A Store must never retry the callback. Effects may have escaped before a
// version conflict, cancellation, or commit failure is discovered. Retrying is
// therefore a domain decision, not a storage convenience. A caller that does
// retry should reload first and use a stable idempotency key. An outbox should
// enforce that key uniquely so a repeated command cannot enqueue two outbox
// records. Relays and consumers still need idempotency because broker delivery
// is commonly at least once.
//
// Context cancellation does not prove that a commit did not happen: it can
// race the storage engine's commit point. Treat an ambiguous cancellation or
// commit error as an unknown outcome and reload before retrying.
//
// The package supplies two adapters: [MemoryStore] for local execution and
// tests, and [FuncStore] for binding an application transaction implementation.
// [Fire] reports the Store's state result. [Step] additionally exposes whether
// the transition callback ran, its From and attempted To, and whether the Store
// confirmed success. A failed Store result is never treated as proof that an
// external commit did not occur.
package persist
