package supervised

import (
	"context"
	"fmt"
	"runtime/debug"
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
	default:
		return fmt.Sprintf("RecordKind(%d)", uint8(kind))
	}
}

// Record is one immutable, sequenced Supervisor lifecycle outcome. Seq is
// authoritative ordering; At is for correlation with external evidence.
type Record[S, E comparable] struct {
	Seq         uint64
	At          time.Time
	Kind        RecordKind
	ExecutionID string
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
}

// Recorder persists or exports ordered lifecycle Records. A failure never
// rewrites an already-completed physical or logical outcome; it is retained in
// Status.RecorderError and the in-memory Records history remains available.
type Recorder[S, E comparable] func(context.Context, Record[S, E]) error

// Journal durably stores an in-doubt Snapshot before an external Issue
// callback can run. Adapters must make repeated saves idempotent.
type Journal[S, E comparable] func(context.Context, Snapshot[S, E]) error

// Options configures time and lifecycle recording for a Supervisor.
type Options[S, E comparable] struct {
	Limits          Limits
	Clock           Clock
	Recorder        Recorder[S, E]
	RecorderTimeout time.Duration
	Journal         Journal[S, E]
	// RequireJournal makes construction fail unless Journal is configured.
	RequireJournal bool
}

func (s *Supervisor[S, E, T]) recordResult(kind RecordKind, result Result[S, E]) {
	s.recordMu.Lock()
	defer s.recordMu.Unlock()

	s.mu.Lock()
	s.nextRecord++
	record := Record[S, E]{
		Seq: s.nextRecord, At: s.clock.Now(), Kind: kind,
		ExecutionID: s.executionID, Operation: result.Operation, Attempt: result.AttemptKey,
		Revision: result.Revision, Mode: s.mode, Phase: result.Phase,
		Change: Change[S, E]{Attempt: Attempt[S, E]{
			DefinitionID: result.DefinitionID, ExecutionID: result.ExecutionID,
			ID: result.Attempt, Revision: result.Revision, StartedAt: result.StartedAt,
			From: result.From, Event: result.Event,
		}, TransitionID: result.TransitionID, To: result.To},
		Selected: result.Selected, Issued: result.IssueCompleted,
		Verified: result.Verified, Committed: result.Committed,
		Faulted: result.Faulted, Uncertain: result.Uncertain,
	}
	if result.Err != nil {
		record.CauseText = result.Err.Error()
	}
	s.records = append(s.records, record)
	recorder := s.recorder
	s.mu.Unlock()

	if recorder == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.recorderTimeout)
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
		done <- recorder(ctx, record)
		returned = true
	}()
	var failure error
	select {
	case failure = <-done:
	case <-ctx.Done():
		failure = ctx.Err()
	}
	if failure != nil {
		s.mu.Lock()
		s.recorderError = failure.Error()
		s.mu.Unlock()
	}
}

// Records returns an immutable copy of every lifecycle Record retained by this
// process. Durable incident history should also be written through Recorder.
func (s *Supervisor[S, E, T]) Records() []Record[S, E] {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Record[S, E](nil), s.records...)
}
