package queued

import (
	"context"
	"errors"
	"iter"
	goruntime "runtime"
	"sync"
	"time"

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

	// ErrWrongRuntime reports Enqueue with a callback context owned by a
	// different Runtime.
	ErrWrongRuntime = errors.New("queued: context belongs to another runtime")
	// ErrRootLimit reports admission beyond a Runtime's outstanding-root limit.
	ErrRootLimit = errors.New("queued: outstanding root limit reached")
	// ErrRunLimit reports admission beyond one Run's cumulative event limit.
	ErrRunLimit = errors.New("queued: run event limit reached")
	// ErrInvalidLimits reports a non-positive queue resource limit.
	ErrInvalidLimits = errors.New("queued: limits must be positive")
)

const (
	DefaultMaxRoots     = 1024
	DefaultMaxRunEvents = 4096
)

// Limits bounds outstanding external roots and cumulative events accepted by
// one Run. MaxRunEvents includes the root event itself.
type Limits struct {
	MaxRoots     int
	MaxRunEvents int
}

// Options configures a Runtime. Zero Limits selects the default bounds;
// otherwise both limits must be positive. Observers are copied at construction.
type Options[S, E comparable, T any] struct {
	Limits    Limits
	Observers []statemachine.Observer[S, E, T]
}

// PanicInfo preserves the most recent Guard or Do panic's originating stack.
// Stack is limited to 64 KiB and may be truncated. The original panic value is
// re-panicked by Fire rather than retained by the Runtime.
type PanicInfo[E comparable] struct {
	Event E
	At    time.Time
	Stack string
}

var defaultLimits = Limits{MaxRoots: DefaultMaxRoots, MaxRunEvents: DefaultMaxRunEvents}

// A Runtime owns the current state of one aggregate and serializes events for
// it. Its Machine remains immutable and may be shared with other runtimes.
//
// The zero Runtime is ready to use. It starts in the zero state with a zero
// Machine, so every event is refused until a Runtime constructed by New is
// used instead. A Runtime must not be copied after first use.
//
// Each external Fire is one root cascade. External roots run in FIFO order;
// events appended by Runtime.Enqueue finish before the next root begins. Runtime does
// not keep a permanent worker goroutine: the goroutine that drains roots exits
// whenever the root queue becomes empty.
type Runtime[S, E comparable, T any] struct {
	mu        sync.Mutex
	instance  statemachine.Instance[S, E, T]
	roots     []*root[E, T, S]
	running   bool
	limits    Limits
	rootCount int
	observers []statemachine.Observer[S, E, T]
	seq       uint64
	lastPanic *PanicInfo[E]
}

// Status is one atomic Runtime scheduling snapshot.
type Status[S comparable] struct {
	State            S
	Running          bool
	OutstandingRoots int
	Limits           Limits
}

// New constructs a Runtime in initial state. A nil machine means the zero
// Machine, which refuses every event.
func New[S, E comparable, T any](machine *statemachine.Machine[S, E, T], initial S) *Runtime[S, E, T] {
	return newRuntime(machine, initial, defaultLimits, nil)
}

// NewWithOptions constructs a Runtime with independently configurable bounds
// and observers. Construction emits no observations. Nil observers are ignored.
func NewWithOptions[S, E comparable, T any](
	machine *statemachine.Machine[S, E, T], initial S, options Options[S, E, T],
) (*Runtime[S, E, T], error) {
	limits := options.Limits
	if limits == (Limits{}) {
		limits = defaultLimits
	}
	if limits.MaxRoots <= 0 || limits.MaxRunEvents <= 0 {
		return nil, ErrInvalidLimits
	}
	return newRuntime(machine, initial, limits, options.Observers), nil
}

// NewWithLimits constructs a Runtime with explicit finite resource limits.
func NewWithLimits[S, E comparable, T any](
	machine *statemachine.Machine[S, E, T],
	initial S,
	limits Limits,
) (*Runtime[S, E, T], error) {
	if limits.MaxRoots <= 0 || limits.MaxRunEvents <= 0 {
		return nil, ErrInvalidLimits
	}
	return newRuntime(machine, initial, limits, nil), nil
}

// NewWithObservers is like [New] and attaches observers for the lifetime of
// the Runtime. Construction and restoration emit no observations. Nil
// observers are ignored.
func NewWithObservers[S, E comparable, T any](
	machine *statemachine.Machine[S, E, T],
	initial S,
	observers ...statemachine.Observer[S, E, T],
) *Runtime[S, E, T] {
	return newRuntime(machine, initial, defaultLimits, observers)
}

func newRuntime[S, E comparable, T any](
	machine *statemachine.Machine[S, E, T],
	initial S,
	limits Limits,
	observers []statemachine.Observer[S, E, T],
) *Runtime[S, E, T] {
	runtime := &Runtime[S, E, T]{
		limits: limits, observers: copyObservers(observers),
	}
	runtime.instance = *statemachine.NewInstance(machine, initial)
	return runtime
}

// State reports the last committed state. While a Guard or Do is running it
// continues to report the state that event started from; the destination is
// published only after Do returns nil.
func (r *Runtime[S, E, T]) State() S {
	return r.instance.State()
}

// LastPanic returns a snapshot of the most recent Guard or Do panic, if any.
// Only one record is retained; a later root's panic replaces it. Successful
// roots do not erase it. This diagnostic does not change panic propagation.
func (r *Runtime[S, E, T]) LastPanic() (PanicInfo[E], bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastPanic == nil {
		return PanicInfo[E]{}, false
	}
	return *r.lastPanic, true
}

// Status reports committed state, scheduler activity, admitted roots, and
// configured resource bounds atomically.
func (r *Runtime[S, E, T]) Status() Status[S] {
	r.mu.Lock()
	defer r.mu.Unlock()
	limits := r.limits
	if limits.MaxRoots == 0 {
		limits = defaultLimits
	}
	return Status[S]{
		State: r.instance.State(), Running: r.running,
		OutstandingRoots: r.rootCount, Limits: limits,
	}
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
// runnable. LastPanic preserves its originating stack. A callback that calls runtime.Goexit instead returns
// [ErrExecutionStopped]; see that error for the legacy nil-panic exception.
//
// Fire passes an execution context to Guards and effects. Calling Fire on any
// Runtime with that active context reports ErrReentrant. Never replace that
// context and call Fire synchronously from a callback: doing so defeats the
// detection and can deadlock behind the root that is waiting for the callback.
// Call this Runtime's Enqueue with the supplied context for same-runtime follow-ups instead.
func (r *Runtime[S, E, T]) Fire(ctx context.Context, event E, data T) (S, error) {
	if x, ok := executionFrom(ctx); ok && x.active() {
		return r.State(), ErrReentrant
	}

	if err := ctx.Err(); err != nil {
		return r.State(), err
	}
	req := &root[E, T, S]{
		ctx:   ctx,
		event: event,
		data:  data,
		done:  make(chan outcome[S], 1),
	}

	r.mu.Lock()
	if r.limits.MaxRoots == 0 {
		r.limits = defaultLimits
	}
	if r.rootCount >= r.limits.MaxRoots {
		state := r.instance.State()
		r.mu.Unlock()
		return state, ErrRootLimit
	}
	r.roots = append(r.roots, req)
	r.rootCount++
	start := !r.running
	if start {
		r.running = true
	}
	r.mu.Unlock()

	if start {
		go r.drain()
	}

	var result outcome[S]
	select {
	case result = <-req.done:
	case <-ctx.Done():
		r.mu.Lock()
		removed := r.removeRootLocked(req)
		if removed {
			r.rootCount--
		}
		r.mu.Unlock()
		if removed {
			return r.State(), ctx.Err()
		}
		result = <-req.done
	}
	if result.panicked {
		panic(result.panicValue)
	}
	return result.state, result.err
}

func (r *Runtime[S, E, T]) removeRootLocked(target *root[E, T, S]) bool {
	for index, candidate := range r.roots {
		if candidate != target {
			continue
		}
		copy(r.roots[index:], r.roots[index+1:])
		r.roots[len(r.roots)-1] = nil
		r.roots = r.roots[:len(r.roots)-1]
		return true
	}
	return false
}

// Permitted reports an eager snapshot of the events accepted in the last
// committed state, paired with their destinations. Guards run before
// Permitted returns; ranging the returned iterator runs no guards and observes
// no later state change. No Runtime lock is held while Guards run, so Permitted
// can overlap an executing callback; callers must synchronize mutable data in
// T and keep Guards pure.
func (r *Runtime[S, E, T]) Permitted(ctx context.Context, data T) iter.Seq2[E, S] {
	return r.instance.Permitted(ctx, data)
}

// Enqueue appends event to this Runtime's cascade identified by ctx. It does not wait for
// the event to run. The event receives ctx when it later runs, so cancellation
// of either the root context or this context skips it and aborts the remaining
// follow-ups in that root. Enqueue must complete before the callback returns;
// if a callback starts enqueueing goroutines, it must join them before return.
//
// Enqueue reports ErrWrongRuntime when ctx belongs to another Runtime and
// ErrNotRunning when its Run is absent or complete.
func (r *Runtime[S, E, T]) Enqueue(ctx context.Context, event E, data T) error {
	x, ok := executionFrom(ctx)
	if !ok || !x.active() {
		return ErrNotRunning
	}
	if x.owner != r {
		return ErrWrongRuntime
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
	mu       sync.Mutex
	active   bool
	pending  []item[E, T]
	accepted int
	maximum  int
}

func (c *cascade[E, T]) enqueue(ctx context.Context, event E, data T) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.active {
		return ErrNotRunning
	}
	if c.accepted >= c.maximum {
		return ErrRunLimit
	}
	c.pending = append(c.pending, item[E, T]{ctx: ctx, event: event, data: data})
	c.accepted++
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
	owner   any
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

		result := r.execute(req)
		r.mu.Lock()
		r.rootCount--
		r.mu.Unlock()
		req.done <- result
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
	c := &cascade[E, T]{active: true, accepted: 1, maximum: r.limits.MaxRunEvents}
	var run uint64
	x := &execution{owner: r, active: c.isActive}
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
	var current item[E, T]
	defer func() {
		recovered := recover()
		if completed {
			return
		}
		c.abort()
		result = outcome[S]{state: r.State(), err: ErrExecutionStopped}
		if recovered != nil {
			stack := make([]byte, 64<<10)
			stack = stack[:goruntime.Stack(stack, false)]
			r.mu.Lock()
			r.lastPanic = &PanicInfo[E]{Event: current.event, At: time.Now(), Stack: string(stack)}
			r.mu.Unlock()
			result = outcome[S]{
				state:      r.State(),
				panicked:   true,
				panicValue: recovered,
			}
		}
	}()

	execCtx := context.WithValue(req.ctx, executionKey{}, x)
	current = item[E, T]{ctx: execCtx, event: req.event, data: req.data}
	var observerFailures []error

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
		to, err := r.instance.Fire(current.ctx, current.event, current.data)
		if err != nil {
			c.abort()
			result = outcome[S]{state: r.State(), err: err}
			break
		}

		var step uint64
		var observers []statemachine.Observer[S, E, T]
		r.mu.Lock()
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
			observerCtx := observationContext(current.ctx, r, c.isActive)
			if err := deliverTransitionObservations(
				observers, observerCtx, step, run, from, to, current.event, current.data,
			); err != nil {
				observerFailures = append(observerFailures, err)
			}
		}

		var ok bool
		current, ok = c.next()
		if !ok {
			result = outcome[S]{state: to}
			break
		}
	}
	if len(observerFailures) != 0 {
		result.err = errors.Join(result.err, errors.Join(observerFailures...))
	}
	completed = true
	return result
}
