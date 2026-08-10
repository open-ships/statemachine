package supervised

import "time"

// Timer is the stoppable notification returned by Clock.AfterFunc.
type Timer interface {
	Stop() bool
}

// Clock supplies wall/monotonic time and operation/verification deadline
// notification. Verification timers are notification only: Verify also checks
// the stored deadline under the Supervisor lock before it admits work, so a
// late, early, repeated, or absent timer callback can delay fault latching
// but can never admit expired work.
//
// Implementations must honor this contract; the Supervisor calls Now,
// AfterFunc, and Timer.Stop while holding its internal lock:
//
//   - Now, AfterFunc, and Stop must be non-blocking. A Clock that performs
//     I/O — a network time source, a contended shared fake — blocks every
//     Supervisor operation, including Trip.
//   - AfterFunc must never invoke its callback synchronously, and the
//     callback must be dispatched on a goroutine that holds no lock the
//     Clock acquires in Now, AfterFunc, or Stop. A synchronous or
//     lock-holding dispatch deadlocks against the Supervisor lock the timer
//     callback re-acquires.
//   - Now must be monotone non-decreasing within one process. Deadline
//     arithmetic preserves Go's monotonic reading; evidentiary timestamps
//     (Record.At, Snapshot.RecordedAt, Fault.OccurredAt) are wall time, and
//     their order across a wall-clock step is not trustworthy — Record.Seq
//     is the authoritative order.
//   - Implementations must not call Supervisor methods from inside Now,
//     AfterFunc, or Stop.
//
// A test Clock that advances time manually satisfies the contract by
// dispatching due callbacks from the test goroutine after releasing its own
// internal lock.
type Clock interface {
	Now() time.Time
	AfterFunc(time.Duration, func()) Timer
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) AfterFunc(duration time.Duration, callback func()) Timer {
	return time.AfterFunc(duration, callback)
}

func normalizedClock(clock Clock) Clock {
	if clock == nil {
		return systemClock{}
	}
	return clock
}
