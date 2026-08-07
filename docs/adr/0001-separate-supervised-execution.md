# Keep supervised execution separate from existing state owners

Safety-adjacent execution uses a new `supervised` module instead of changing Instance, Runtime, or Statechart semantics. Those existing modules intentionally recover after callback failure and combine effects with their documented commit points; a Supervisor instead bounds one Attempt, separates issue from verification, and latches the first Fault without claiming to be a safety-rated controller.
