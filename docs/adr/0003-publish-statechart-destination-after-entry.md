# Publish a Statechart destination after entry succeeds

A Statechart Instance keeps Source as its last committed state while exit, transition effect, and entry actions run. Destination is published only after every entry action succeeds. Entry failure, panic, or runtime.Goexit therefore leaves Source committed and emits no successful-looking Observation batch.

ActionError identifies the failing phase, state, action index, and number of completed lifecycle actions. It does not claim rollback: external effects from completed exit or entry actions may remain. Hazardous actuation still belongs in supervised execution behind independent protection.
