package supervised

import (
	"context"
	"fmt"
	"math"
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
	seq       uint64
	at        time.Time
	mode      Mode
	durable   bool
	ok        bool
	exhausted bool
}

// Record is one immutable, sequenced Supervisor lifecycle outcome.
//
// (ExecutionID, IncarnationID, Seq) is the durable Record identity. Seq is
// assigned atomically with the decision, and orders decisions within an
// incarnation. A fresh random IncarnationID distinguishes every restoration,
// even when the latest counters were never saved. Restarts is diagnostic.
// At and Mode are sampled at the decision instant; wall time is for
// correlation only. Change retains the original accepted command, while
// Revision and OperationStartedAt/OperationCompletedAt describe this outcome.
// OperationCompletedAt marks decision completion before adapter delivery.
type Record[S, E comparable] struct {
	Seq           uint64
	At            time.Time
	Kind          RecordKind
	ExecutionID   string
	IncarnationID string
	Restarts      uint64
	Operation     uint64
	Attempt       AttemptID
	Revision      uint64
	Mode          Mode
	Phase         Phase
	Change        Change[S, E]
	Selected      bool
	Issued        bool
	Verified      bool
	Committed     bool
	Faulted       bool
	Uncertain     bool
	CauseText     string
	// Evidence is the verbatim Decision text of an Adjudicate outcome.
	Evidence             string
	Adjudication         Adjudication
	OperationStartedAt   time.Time
	OperationCompletedAt time.Time
}

// Recorder persists or exports lifecycle Records. A failure never rewrites an
// already-completed physical or logical outcome; it is retained in
// Status.RecorderError, counted in Status.RecorderFailures, and the in-memory
// Records history remains available.
//
// Delivery admits one callback at a time and bounds the caller's wait by
// RecorderTimeout. A timed-out callback retains its lease until it ends;
// subsequent delivery fails with ErrDeliveryBusy. At most one Recorder
// goroutine can remain blocked per Supervisor. Records are retained in memory
// before delivery; no automatic retry occurs. Arrival order is not causal
// order: use IncarnationID and Seq to identify and order records.
type Recorder[S, E comparable] func(context.Context, Record[S, E]) error

// Journal durably stores lifecycle Snapshots. An in-doubt preparation must
// succeed before an external Issue callback runs. Closure delivery follows
// startup, accepted guard refusals, commits, fresh faults, and successful
// recovery/adjudication. Each snapshot is captured after admission to the
// Journal gate; an older write cannot overlap a later preparation.
//
// Saves must be idempotent. One callback may run at a time, including after a
// timeout; busy delivery fails immediately. Failed preparation refuses Issue
// and latches a Fault. Failed or skipped closure preserves the decided outcome
// and is exposed through Status.JournalError and RecordJournalError. The
// latest durable snapshot can be clean or in doubt: startup reconciliation is
// always required. There is no automatic retry and no cross-process fencing.
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
	if s.nextRecord == math.MaxUint64 {
		return recordStamp{exhausted: true}
	}
	s.nextRecord++
	return recordStamp{seq: s.nextRecord, at: s.clock.Now(), mode: s.mode, ok: true}
}

// appendRecordLocked retains the newest causal identities even when an older
// decision reaches publication late. It is called with s.mu held.
func (s *Supervisor[S, E, T]) appendRecordLocked(record Record[S, E]) {
	index, exists := slices.BinarySearchFunc(s.records, record.Seq, func(r Record[S, E], seq uint64) int {
		if r.Seq < seq {
			return -1
		}
		if r.Seq > seq {
			return 1
		}
		return 0
	})
	if exists {
		return
	}
	if len(s.records) == s.maxRecords {
		if s.droppedRecords < math.MaxUint64 {
			s.droppedRecords++
		}
		if index == 0 {
			return
		}
		copy(s.records, s.records[1:])
		s.records = s.records[:len(s.records)-1]
		index--
	}
	s.records = append(s.records, Record[S, E]{})
	copy(s.records[index+1:], s.records[index:])
	s.records[index] = record
}

func (s *Supervisor[S, E, T]) recordResult(kind RecordKind, result Result[S, E]) {
	// Application error formatting is outside the state lock.
	causeText := ""
	if result.Err != nil {
		causeText = safeErrorText(result.Err)
	}
	s.mu.Lock()
	stamp := result.stamp
	if !stamp.ok && !stamp.exhausted {
		stamp = s.stampLocked()
	}
	if stamp.exhausted {
		if s.droppedRecords < math.MaxUint64 {
			s.droppedRecords++
		}
		s.mu.Unlock()
		return
	}
	record := Record[S, E]{
		Seq: stamp.seq, At: stamp.at, Kind: kind,
		ExecutionID: s.executionID, IncarnationID: s.incarnationID, Restarts: s.restarts,
		Operation: result.Operation, Attempt: result.AttemptKey,
		Revision: result.Revision, Mode: stamp.mode, Phase: result.Phase,
		Change:   result.Change,
		Selected: result.Selected, Issued: result.IssueCompleted,
		Verified: result.Verified, Committed: result.Committed,
		Faulted: result.Faulted, Uncertain: result.Uncertain,
		Evidence: result.Evidence, Adjudication: result.Adjudication,
		OperationStartedAt: result.StartedAt, OperationCompletedAt: result.CompletedAt,
		CauseText: causeText,
	}
	s.appendRecordLocked(record)
	recorder := s.recorder
	timeout := s.recorderTimeout
	s.mu.Unlock()
	if recorder == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	failure := s.deliverAdapter(ctx, &s.recorderGate, result.owner, false, func(ctx context.Context) error { return recorder(ctx, record) })
	if failure != nil {
		message := safeErrorText(failure)
		s.mu.Lock()
		s.recorderError = message
		if s.recorderFailures < math.MaxUint64 {
			s.recorderFailures++
		}
		s.mu.Unlock()
	}
}

// Records returns an immutable copy of the newest retained causal Records,
// ordered by Seq. Status.RecordsDropped counts evicted or exhausted history.
func (s *Supervisor[S, E, T]) Records() []Record[S, E] {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.records)
}
