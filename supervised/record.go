package supervised

import (
	"context"
	"fmt"
	"runtime/debug"
	"slices"
	"time"
)

// RecordKind identifies a durable Supervisor lifecycle outcome.
type RecordKind uint8

const (
	RecordStart RecordKind = iota
	RecordIssue
	RecordVerify
	RecordTrip
	RecordRecover
	RecordVerificationExpired
	RecordSecondaryCause
	RecordAdjudicate
	RecordJournalError
)

func (kind RecordKind) String() string {
	switch kind {
	case RecordStart:
		return "start"
	case RecordIssue:
		return "issue"
	case RecordVerify:
		return "verify"
	case RecordTrip:
		return "trip"
	case RecordRecover:
		return "recover"
	case RecordVerificationExpired:
		return "verification expired"
	case RecordSecondaryCause:
		return "secondary cause"
	case RecordAdjudicate:
		return "adjudicate"
	case RecordJournalError:
		return "journal error"
	default:
		return fmt.Sprintf("RecordKind(%d)", uint8(kind))
	}
}

// recordStamp is the causal identity of one lifecycle outcome. It is assigned
// under the Supervisor lock at the same instant the outcome takes effect, so
// Seq order equals decision order even when delivery is later reordered by
// goroutine scheduling. durable marks outcomes that changed the state a
// Journal must reflect: a commit, a freshly latched Fault, or a completed
// recovery or adjudication.
type recordStamp struct {
	seq     uint64
	at      time.Time
	mode    Mode
	durable bool
	ok      bool
}

// Record is one immutable, sequenced Supervisor lifecycle outcome.
//
// (ExecutionID, Restarts, Seq) is the durable Record identity: Seq is
// assigned atomically with the state decision it describes, so Seq order is
// causal order, and Restarts distinguishes process incarnations so a restored
// execution can never reuse an identity already written by an earlier one. At
// and Mode are sampled at that same decision instant. At is wall time for
// correlation with external evidence only; across a clock step its order is
// not trustworthy, and Seq remains authoritative.
type Record[S, E comparable] struct {
	Seq         uint64
	At          time.Time
	Kind        RecordKind
	ExecutionID string
	Restarts    uint64
	Operation   uint64
	Attempt     AttemptID
	Revision    uint64
	Mode        Mode
	Phase       Phase
	Change      Change[S, E]
	Selected    bool
	Issued      bool
	Verified    bool
	Committed   bool
	Faulted     bool
	Uncertain   bool
	CauseText   string
	// Evidence is the verbatim Decision text of an Adjudicate outcome.
	Evidence string
}

// Recorder persists or exports lifecycle Records. A failure never rewrites an
// already-completed physical or logical outcome; it is retained in
// Status.RecorderError, counted in Status.RecorderFailures, and the in-memory
// Records history remains available.
//
// Delivery is serialized and each call is bounded by RecorderTimeout, but the
// bound covers the caller's wait, not the Recorder goroutine: a Recorder that
// ignores cancellation leaks one goroutine per abandoned delivery. Under
// concurrent operations, delivery order can differ from decision order; the
// Record's (Restarts, Seq) identity is authoritative, never arrival order.
type Recorder[S, E comparable] func(context.Context, Record[S, E]) error

// Journal durably stores the Supervisor's lifecycle Snapshots. It receives an
// in-doubt Snapshot before an external Issue callback may run, and a closure
// Snapshot after every outcome that changes durable truth: a logical or
// verified commit, a freshly latched Fault, and a completed recovery or
// adjudication. Journal writes are serialized in decision order and each
// snapshot is captured at write time, so the stored state converges to the
// latest decision.
//
// Adapters must make repeated saves idempotent. A failed preparation write
// refuses the Issue and latches a Fault. A failed closure write never
// rewrites the completed outcome: it is retained in Status.JournalError and
// recorded as a RecordJournalError, and the stale journal makes the next
// restore conservatively in-doubt, which is the safe direction.
type Journal[S, E comparable] func(context.Context, Snapshot[S, E]) error

// Options configures time, durability, and lifecycle recording for a
// Supervisor.
type Options[S, E comparable] struct {
	Limits Limits
	Clock  Clock
	// Recorder receives every lifecycle Record. RecorderTimeout bounds each
	// delivery and each Journal closure write; zero means
	// Limits.OperationTimeout.
	Recorder        Recorder[S, E]
	RecorderTimeout time.Duration
	Journal         Journal[S, E]
	// RequireJournal makes construction fail unless Journal is configured,
	// even for a Machine with no external transition.
	RequireJournal bool
	// Unjournaled explicitly acknowledges volatile in-doubt state for a
	// Machine with external Issue actions: without a Journal, a restart
	// cannot prove whether issued work reached a controller. It exists for
	// tests and non-hazardous work; never set it for work that can move
	// machinery. Construction fails with ErrJournalRequired when a Machine
	// declares external transitions and neither Journal nor Unjournaled is
	// set.
	Unjournaled bool
}

// stampLocked assigns the next causal record identity. It must be called with
// s.mu held, at the instant the outcome it describes takes effect.
func (s *Supervisor[S, E, T]) stampLocked() recordStamp {
	s.nextRecord++
	return recordStamp{seq: s.nextRecord, at: s.clock.Now(), mode: s.mode, ok: true}
}

// appendRecordLocked retains record in the bounded in-process ring. It must
// be called with s.mu held. When the ring is full the oldest entry is
// overwritten and the loss is counted; Seq already provides authoritative
// ordering, so dropping oldest-first loses no identity.
func (s *Supervisor[S, E, T]) appendRecordLocked(record Record[S, E]) {
	if len(s.records) < s.maxRecords {
		s.records = append(s.records, record)
		return
	}
	s.records[s.recordHead] = record
	s.recordHead++
	if s.recordHead == s.maxRecords {
		s.recordHead = 0
	}
	s.droppedRecords++
}

func (s *Supervisor[S, E, T]) recordResult(kind RecordKind, result Result[S, E]) {
	s.recordMu.Lock()
	defer s.recordMu.Unlock()

	s.mu.Lock()
	stamp := result.stamp
	if !stamp.ok {
		stamp = s.stampLocked()
	}
	record := Record[S, E]{
		Seq: stamp.seq, At: stamp.at, Kind: kind,
		ExecutionID: s.executionID, Restarts: s.restarts,
		Operation: result.Operation, Attempt: result.AttemptKey,
		Revision: result.Revision, Mode: stamp.mode, Phase: result.Phase,
		Change: Change[S, E]{Attempt: Attempt[S, E]{
			DefinitionID: result.DefinitionID, ExecutionID: result.ExecutionID,
			ID: result.Attempt, Revision: result.Revision, StartedAt: result.StartedAt,
			From: result.From, Event: result.Event,
		}, TransitionID: result.TransitionID, To: result.To},
		Selected: result.Selected, Issued: result.IssueCompleted,
		Verified: result.Verified, Committed: result.Committed,
		Faulted: result.Faulted, Uncertain: result.Uncertain,
		Evidence: result.Evidence,
	}
	if result.Err != nil {
		record.CauseText = result.Err.Error()
	}
	s.appendRecordLocked(record)
	recorder := s.recorder
	timeout := s.recorderTimeout
	s.mu.Unlock()

	if recorder == nil {
		return
	}
	failure := deliverBounded(timeout, func(ctx context.Context) error {
		return recorder(ctx, record)
	})
	if failure != nil {
		s.mu.Lock()
		s.recorderError = failure.Error()
		s.recorderFailures++
		s.mu.Unlock()
	}
}

// deliverBounded runs deliver in a contained goroutine and waits at most
// timeout. A panic or runtime.Goexit is converted to an error; on timeout the
// goroutine is abandoned and completes in the background.
func deliverBounded(timeout time.Duration, deliver func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		returned := false
		defer func() {
			if returned {
				return
			}
			if value := recover(); value != nil {
				done <- &PanicError{Value: value, Stack: string(debug.Stack())}
			} else {
				done <- ErrExecutionStopped
			}
		}()
		done <- deliver(ctx)
		returned = true
	}()
	select {
	case failure := <-done:
		return failure
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Records returns an immutable copy of the lifecycle Records retained by this
// process, ordered by Seq. Retention is bounded by Limits.MaxRecords and
// Status.RecordsDropped counts evicted history; durable incident history
// should also be written through Recorder.
func (s *Supervisor[S, E, T]) Records() []Record[S, E] {
	s.mu.Lock()
	records := make([]Record[S, E], 0, len(s.records))
	records = append(records, s.records[s.recordHead:]...)
	records = append(records, s.records[:s.recordHead]...)
	s.mu.Unlock()
	// Concurrent decisions can be appended out of Seq order; Seq is the
	// authoritative causal order, so present it that way.
	slices.SortStableFunc(records, func(a, b Record[S, E]) int {
		switch {
		case a.Seq < b.Seq:
			return -1
		case a.Seq > b.Seq:
			return 1
		default:
			return 0
		}
	})
	return records
}
