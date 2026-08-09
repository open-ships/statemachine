package supervised

import "time"

// Timer is the stoppable notification returned by Clock.AfterFunc.
type Timer interface {
	Stop() bool
}

// Clock supplies wall/monotonic time and operation/verification deadline
// notification. Verification timers are notification only: Verify also checks
// the stored deadline under the Supervisor lock before it admits work.
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
