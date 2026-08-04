package queued

import (
	"context"
	"errors"
	"iter"
	"sync"

	"github.com/open-ships/statemachine"
)

var (
	// ErrReentrant reports a synchronous call to Runtime.Fire made with an
	// active queued execution context. A queued callback must use Enqueue for a
	// same-runtime follow-up and return before another external root can run.
	ErrReentrant = errors.New("queued: reentrant fire")

	// ErrExecutionStopped reports that a Guard or Do terminated its goroutine
	// with runtime.Goexit. It also covers a nil panic under the legacy
	// GODEBUG=panicnil=1 behavior, which recover cannot distinguish from
	// Goexit. The root is aborted and later roots remain runnable.
	ErrExecutionStopped = errors.New("queued: callback stopped its execution goroutine")

	// ErrNotRunning reports that Enqueue's context does not identify a
	// currently running cascade. Execution contexts are valid for enqueueing
	// only until their root Fire completes.
	ErrNotRunning = errors.New("queued: no cascade is running in context")
)

// A Runtime owns the current state of one aggregate and serializes events for
// it. Its Machine remains immutable and may be shared with other runtimes.
//
// The zero Runtime is ready to use. It starts in the zero state with a zero
// Machine, so every event is refused until a Runtime constructed by New is
// used instead. A Runtime must not be copied after first use.
//
// Each external Fire is one root cascade. External roots run in FIFO order;
// events appended by Enqueue finish before the next root begins. Runtime does
// not keep a permanent worker goroutine: the goroutine that drains roots exits
// whenever the root queue becomes empty.
type Runtime[S, E comparable, T any] struct {
	mu        sync.Mutex
	machine   *statemachine.Machine[S, E, T]
	state     S
	roots     []*root[E, T, S]
	running   bool
	observers []statemachine.Observer[S, E, T]
	seq       uint64
}

// New constructs a Runtime in initial state. A nil machine means the zero
// Machine, which refuses every event.
func New[S, E comparable, T any](machine *statemachine.Machine[S, E, T], initial S) *Runtime[S, E, T] {
	return newRuntime(machine, initial, nil)
}

// NewWithObservers is like [New] and attaches observers for the lifetime of
// the Runtime. Construction and restoration emit no observations. Nil
// observers are ignored.
func NewWithObservers[S, E comparable, T any](
	machine *statemachine.Machine[S, E, T],
	initial S,
	observers ...statemachine.Observer[S, E, T],
) *Runtime[S, E, T] {
	return newRuntime(machine, initial, observers)
}

func newRuntime[S, E comparable, T any](
	machine *statemachine.Machine[S, E, T],
	initial S,
	observers []statemachine.Observer[S, E, T],
) *Runtime[S, E, T] {
	return &Runtime[S, E, T]{
		machine: machine, state: initial, observers: copyObservers(observers),
	}
}

// State reports the last committed state. While a Guard or Do is running it
// continues to report the state that event started from; the destination is
// published only after Do returns nil.
func (r *Runtime[S, E, T]) State() S {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

// Fire appends an external root event and waits for that root's entire cascade
// to finish. Roots are serialized in arrival order.
//
// The context is checked immediately before the root and before every
// follow-up. If it is canceled before an event begins, that event is skipped,
// the root's remaining follow-ups are discarded, and Fire reports ctx.Err().
// Cancellation does not interrupt a Guard or Do that ignores its context.
//
// On success Fire returns the state after all of this root's follow-ups. An
// event refusal or effect error returns the state after any earlier successful
// events in this root and discards its remaining follow-ups without affecting
// later roots. A panic does the same cleanup and is re-panicked with its
// original value in the goroutine that called Fire; later queued roots remain
// runnable. A callback that calls runtime.Goexit instead returns
// [ErrExecutionStopped]; see that error for the legacy nil-panic exception.
//
// Fire passes an execution context to Guards and effects. Calling Fire on any
// Runtime with that active context reports ErrReentrant. Never replace that
// context and call Fire synchronously from a callback: doing so defeats the
// detection and can deadlock behind the root that is waiting for the callback.
// Call Enqueue with the supplied context for same-runtime follow-ups instead.
func (r *Runtime[S, E, T]) Fire(ctx context.Context, event E, data T) (S, error) {
	if x, ok := executionFrom(ctx); ok && x.active() {
		return r.State(), ErrReentrant
	}

	req := &root[E, T, S]{
		ctx:   ctx,
		event: event,
		data:  data,
		done:  make(chan outcome[S], 1),
	}

	r.mu.Lock()
	r.roots = append(r.roots, req)
	start := !r.running
	if start {
		r.running = true
	}
	r.mu.Unlock()

	if start {
		go r.drain()
	}

	result := <-req.done
	if result.panicked {
		panic(result.panicValue)
	}
	return result.state, result.err
}

// Permitted reports an eager snapshot of the events accepted in the last
// committed state, paired with their destinations. Guards run before
// Permitted returns; ranging the returned iterator runs no guards and observes
// no later state change. No Runtime lock is held while Guards run, so Permitted
// can overlap an executing callback; callers must synchronize mutable data in
// T and keep Guards pure.
func (r *Runtime[S, E, T]) Permitted(ctx context.Context, data T) iter.Seq2[E, S] {
	r.mu.Lock()
	state := r.state
	machine := r.machine
	r.mu.Unlock()

	type pair struct {
		event E
		to    S
	}
	var snapshot []pair
	if machine == nil {
		var zero statemachine.Machine[S, E, T]
		for event, to := range zero.Permitted(ctx, state, data) {
			snapshot = append(snapshot, pair{event, to})
		}
	} else {
		for event, to := range machine.Permitted(ctx, state, data) {
			snapshot = append(snapshot, pair{event, to})
		}
	}

	return func(yield func(E, S) bool) {
		for _, p := range snapshot {
			if !yield(p.event, p.to) {
				return
			}
		}
	}
}

// Enqueue appends event to the cascade identified by ctx. It does not wait for
// the event to run. The event receives ctx when it later runs, so cancellation
// of either the root context or this context skips it and aborts the remaining
// follow-ups in that root. Enqueue must complete before the callback returns;
// if a callback starts enqueueing goroutines, it must join them before return.
//
// Enqueue reports ErrNotRunning when ctx did not come from a currently running
// Runtime callback, when that cascade has already finished, or when event and
// data are not assignable to that Runtime's E and T types.
func Enqueue[E comparable, T any](ctx context.Context, event E, data T) error {
	x, ok := executionFrom(ctx)
	if !ok {
		return ErrNotRunning
	}
	return x.enqueue(ctx, event, data)
}

type root[E comparable, T any, S comparable] struct {
	ctx   context.Context
	event E
	data  T
	done  chan outcome[S]
}

type outcome[S comparable] struct {
	state      S
	err        error
	panicked   bool
	panicValue any
}

type item[E comparable, T any] struct {
	ctx   context.Context
	event E
	data  T
}

// cascade has a separate mutex because Enqueue may be called from goroutines
// that the effect joins before returning. The runtime mutex is never held while
// user code runs.
type cascade[E comparable, T any] struct {
	mu      sync.Mutex
	active  bool
	pending []item[E, T]
}

func (c *cascade[E, T]) enqueue(ctx context.Context, event E, data T) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.active {
		return ErrNotRunning
	}
	c.pending = append(c.pending, item[E, T]{ctx: ctx, event: event, data: data})
	return nil
}

func (c *cascade[E, T]) next() (item[E, T], bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) == 0 {
		c.active = false
		return item[E, T]{}, false
	}
	next := c.pending[0]
	c.pending[0] = item[E, T]{}
	c.pending = c.pending[1:]
	return next, true
}

func (c *cascade[E, T]) abort() {
	c.mu.Lock()
	c.active = false
	clear(c.pending)
	c.pending = nil
	c.mu.Unlock()
}

func (c *cascade[E, T]) isActive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.active
}

type executionKey struct{}

type execution struct {
	active  func() bool
	enqueue func(context.Context, any, any) error
}

func executionFrom(ctx context.Context) (*execution, bool) {
	if ctx == nil {
		return nil, false
	}
	x, ok := ctx.Value(executionKey{}).(*execution)
	return x, ok
}

func assign[T any](value any) (T, bool) {
	result, ok := value.(T)
	if ok {
		return result, true
	}

	// An untyped nil converted to any has no dynamic type. It is nevertheless
	// assignable when T itself is an interface (the common Runtime[..., any]
	// case). Typed nil pointers, maps, slices, functions and channels take the
	// ordinary assertion path above.
	if value == nil {
		var zero T
		if any(zero) == nil {
			return zero, true
		}
	}
	var zero T
	return zero, false
}

func (r *Runtime[S, E, T]) drain() {
	for {
		r.mu.Lock()
		if len(r.roots) == 0 {
			r.running = false
			r.roots = nil
			r.mu.Unlock()
			return
		}
		req := r.roots[0]
		r.roots[0] = nil
		r.roots = r.roots[1:]
		r.mu.Unlock()

		req.done <- r.execute(req)
	}
}

// execute isolates a root so runtime.Goexit in user code cannot terminate the
// queue's drain goroutine. The deferred send is the Goexit path: ordinary
// returns set returned first and send the more specific outcome from run.
func (r *Runtime[S, E, T]) execute(req *root[E, T, S]) outcome[S] {
	done := make(chan outcome[S], 1)
	go func() {
		returned := false
		defer func() {
			if !returned {
				done <- outcome[S]{state: r.State(), err: ErrExecutionStopped}
			}
		}()
		result := r.run(req)
		returned = true
		done <- result
	}()
	return <-done
}

func (r *Runtime[S, E, T]) run(req *root[E, T, S]) (result outcome[S]) {
	c := &cascade[E, T]{active: true}
	var run uint64
	x := &execution{active: c.isActive}
	x.enqueue = func(ctx context.Context, event, data any) error {
		typedEvent, ok := assign[E](event)
		if !ok {
			return ErrNotRunning
		}
		typedData, ok := assign[T](data)
		if !ok {
			return ErrNotRunning
		}
		return c.enqueue(ctx, typedEvent, typedData)
	}

	completed := false
	defer func() {
		recovered := recover()
		if completed {
			return
		}
		c.abort()
		result = outcome[S]{state: r.State(), err: ErrExecutionStopped}
		if recovered != nil {
			result = outcome[S]{
				state:      r.State(),
				panicked:   true,
				panicValue: recovered,
			}
		}
	}()

	execCtx := context.WithValue(req.ctx, executionKey{}, x)
	current := item[E, T]{ctx: execCtx, event: req.event, data: req.data}

	for {
		if err := req.ctx.Err(); err != nil {
			c.abort()
			result = outcome[S]{state: r.State(), err: err}
			break
		}
		if err := current.ctx.Err(); err != nil {
			c.abort()
			result = outcome[S]{state: r.State(), err: err}
			break
		}

		from := r.State()
		to, err := r.fireMachine(current.ctx, from, current.event, current.data)
		if err != nil {
			c.abort()
			result = outcome[S]{state: r.State(), err: err}
			break
		}

		var step uint64
		var observers []statemachine.Observer[S, E, T]
		r.mu.Lock()
		r.state = to
		if from != to && len(r.observers) != 0 {
			step = r.seq + 1
			r.seq += 2
			if run == 0 {
				run = step
			}
			observers = r.observers
		}
		r.mu.Unlock()

		if len(observers) != 0 {
			observerCtx := observationContext(current.ctx, c.isActive)
			deliverTransitionObservations(
				observers, observerCtx, step, run, from, to, current.event, current.data,
			)
		}

		var ok bool
		current, ok = c.next()
		if !ok {
			result = outcome[S]{state: to}
			break
		}
	}
	completed = true
	return result
}

func (r *Runtime[S, E, T]) fireMachine(ctx context.Context, from S, event E, data T) (S, error) {
	if r.machine != nil {
		return r.machine.Fire(ctx, from, event, data)
	}
	var zero statemachine.Machine[S, E, T]
	return zero.Fire(ctx, from, event, data)
}
