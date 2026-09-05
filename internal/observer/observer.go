// Package observer owns typed, ordered observation delivery and callback
// failure containment for the execution adapters in this module.
package observer

import "runtime/debug"

// Failure describes a callback that panicked or stopped its goroutine with
// runtime.Goexit. Stack is captured before the callback goroutine unwinds.
type Failure struct {
	Value   any
	Stack   string
	Stopped bool
}

// Call invokes callback in an isolated goroutine. It returns nil only when the
// callback returns normally.
func Call(callback func()) *Failure {
	return <-Start(callback)
}

// Start invokes callback in an isolated goroutine and returns the channel its
// outcome will be delivered on, so a caller can bound its wait. The channel is
// buffered: an abandoned callback still completes and never blocks sending.
func Start(callback func()) <-chan *Failure {
	done := make(chan *Failure, 1)
	go func() {
		returned := false
		defer func() {
			if returned {
				done <- nil
				return
			}
			failure := &Failure{Stack: string(debug.Stack())}
			if value := recover(); value != nil {
				failure.Value = value
			} else {
				failure.Stopped = true
			}
			done <- failure
		}()
		callback()
		returned = true
	}()
	return done
}
