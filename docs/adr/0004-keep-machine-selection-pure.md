# Keep Machine selection pure and effects behind state owners

`Machine.Next` runs Guards and reports the first applicable transition's
destination, but never runs `Do`. The prior `Machine.Fire` interface is removed.
It combined an external effect with a destination returned to caller-owned
state, allowing valid Go code to discard the only state commit after the effect
had already occurred.

Flat effects execute only through a module that owns the corresponding state:
Instance for fail-fast in-memory execution, Runtime for queued execution, or a
Store-backed execution for persistence. Runtime and Store-backed execution use
Instance internally so transition selection, `Do`, and in-process publication
remain local to one implementation.

This is intentionally a breaking interface change. A pure caller-owned query
may still be ignored and therefore accomplish nothing, but it cannot leave an
effect behind. State ownership does not make arbitrary I/O atomic: effects can
remain partial on error, panic, cancellation, or process failure, so durable or
physical work still requires the Store and supervised protocols documented by
their modules.
