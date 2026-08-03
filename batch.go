package litex

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

const (
	// batchFlushInterval is how long buffered writes are held before flushing to a real transaction.
	batchFlushInterval = 10 * time.Second

	// batchFlushTimeout bounds a single flush transaction. Deliberately larger than
	// batchFlushInterval so a backlog is not cancelled mid-transaction, which would drop the batch.
	batchFlushTimeout = 60 * time.Second

	// batchMaxEntries pulls the pending flush forward rather than holding a burst for the full
	// interval. Soft cap: queueBatch never blocks, so writes outpacing the flush still grow past it.
	batchMaxEntries = 1000

	// maxFlushAttempts bounds retries of a failing batch before it is dropped, so one permanently
	// failing statement cannot wedge every later flush behind it and grow the buffer without bound.
	maxFlushAttempts = 3
)

// batchEntry holds a single buffered write operation.
type batchEntry struct {
	stmt string
	args []any
}

// BatchTX is provided to project-specific transaction types via CommitBatch. It exposes QueueExec for
// buffered writes that are flushed to a real transaction by a background timer. It is safe for
// concurrent use, so a callback may fan QueueExec out across goroutines.
type BatchTX struct {
	logger zerolog.Logger

	mu      sync.Mutex
	entries []batchEntry
}

// Logger returns the logger for this transaction.
func (b *BatchTX) Logger() zerolog.Logger {
	return b.logger
}

// QueueExec buffers a write statement for deferred commit rather than executing immediately. It
// always returns nil: execution failures surface via OnBatchFlushError, the logger, or Close. The
// context is not retained; the flush runs under its own timeout.
func (b *BatchTX) QueueExec(_ context.Context, stmt string, args ...any) error {
	b.logger.Debug().Str("stmt", stmt).Any("args", args).Msg("Buffering batch statement")

	b.mu.Lock()
	b.entries = append(b.entries, batchEntry{stmt: stmt, args: args})
	b.mu.Unlock()

	return nil
}

// CommitBatch provides a BatchTX to the given function for buffered writes. Entries queued via
// QueueExec reach the service buffer only once fn returns nil, and are flushed roughly every
// batchFlushInterval and on Close.
//
// The buffer is service-wide, so one failing statement rolls back every entry in that window,
// including other callers'. Use CommitWrite where failure must be attributable to its own caller.
func (s *Service) CommitBatch(_ context.Context, newTx func(*BatchTX) any, fn func(any) error) error {
	if s.connRW == nil {
		return errors.New("database not open")
	}

	b := &BatchTX{logger: *s.logger}
	if err := fn(newTx(b)); err != nil {
		return err
	}

	b.mu.Lock()
	entries := b.entries
	b.mu.Unlock()

	return s.queueBatch(entries...)
}

// OnBatchFlushError registers a handler invoked when an asynchronous batch flush fails. This handler
// and Close's error are the only way to observe such failures, since the write runs long after
// QueueExec returned. When unset, failures are logged.
func (s *Service) OnBatchFlushError(handler func(error)) {
	s.batchMu.Lock()
	s.batchErrHandler = handler
	s.batchMu.Unlock()
}

// queueBatch buffers entries and arms the flush timer if it is not already running. It rejects
// entries once the service is closed, so a write racing shutdown fails loudly instead of being
// buffered for a flush that can never run.
func (s *Service) queueBatch(entries ...batchEntry) error {
	if len(entries) == 0 {
		return nil
	}

	s.batchMu.Lock()
	defer s.batchMu.Unlock()

	if s.closed {
		return errors.New("database not open")
	}

	before := len(s.batchEntries)
	s.batchEntries = append(s.batchEntries, entries...)

	if s.batchTimer == nil {
		s.batchTimer = time.AfterFunc(batchFlushInterval, s.flushBatchAsync)
	}

	// Only on the call that crosses the cap: resetting on every call past it would queue a
	// flushBatchAsync per call, each waiting on connMu to find the buffer already drained.
	if before < batchMaxEntries && len(s.batchEntries) >= batchMaxEntries {
		s.batchTimer.Reset(0)
	}

	return nil
}

// flushBatchAsync is the timer callback; it flushes and routes any error to the handler or logger.
func (s *Service) flushBatchAsync() {
	ctx, cancel := context.WithTimeout(context.Background(), batchFlushTimeout)
	defer cancel()

	err := s.flushBatch(ctx)
	if err == nil {
		return
	}

	s.batchMu.Lock()
	handler := s.batchErrHandler
	s.batchMu.Unlock()

	if handler != nil {
		handler(err)
	} else {
		s.logger.Error().Err(err).Msg("Batch flush failed")
	}
}

// flushBatch takes connMu and flushes. Close, which needs the lock held across both the flush and the
// connection teardown that follows it, calls flushLocked directly.
func (s *Service) flushBatch(ctx context.Context) error {
	s.connMu.Lock()
	defer s.connMu.Unlock()

	return s.flushLocked(ctx)
}

// flushLocked swaps out the buffered entries and writes them in a single transaction. The caller must
// hold connMu; batchMu is taken only to swap the buffer, so queueBatch never waits on database I/O.
// A failed flush re-buffers its entries, up to maxFlushAttempts, and a retried entry can run twice if
// the commit failed after SQLite had applied it.
func (s *Service) flushLocked(ctx context.Context) error {
	// Checked before draining, so a flush with nowhere to write reports rather than discards.
	if s.connRW == nil {
		s.batchMu.Lock()
		buffered := len(s.batchEntries)
		s.batchMu.Unlock()

		if buffered == 0 {
			return nil
		}

		return errors.New("database not open")
	}

	s.batchMu.Lock()
	entries := s.batchEntries
	s.batchEntries = nil
	if s.batchTimer != nil {
		s.batchTimer.Stop()
		s.batchTimer = nil
	}
	s.batchMu.Unlock()

	if len(entries) == 0 {
		return nil
	}

	tx, err := s.connRW.BeginTx(ctx, nil)
	if err != nil {
		s.retryOrDrop(entries)
		return fmt.Errorf("begin batch flush: %w", err)
	}

	for _, e := range entries {
		if _, err = tx.ExecContext(ctx, e.stmt, e.args...); err != nil {
			_ = tx.Rollback()
			s.retryOrDrop(entries)

			return fmt.Errorf("apply batch entry %q: %w", e.stmt, err)
		}
	}

	if err = tx.Commit(); err != nil {
		_ = tx.Rollback()
		s.retryOrDrop(entries)

		return fmt.Errorf("commit batch: %w", err)
	}

	s.batchMu.Lock()
	s.batchAttempts = 0
	s.batchMu.Unlock()

	return nil
}

// retryOrDrop puts a failed flush's entries back at the front of the buffer, preserving write order,
// until they have failed maxFlushAttempts times. It arms the timer directly rather than via
// queueBatch, whose closed check would discard them during a Close-triggered flush.
func (s *Service) retryOrDrop(entries []batchEntry) {
	s.batchMu.Lock()
	defer s.batchMu.Unlock()

	s.batchAttempts++
	if s.batchAttempts >= maxFlushAttempts {
		s.batchAttempts = 0
		return
	}

	s.batchEntries = append(entries, s.batchEntries...)

	if s.batchTimer == nil && !s.closed {
		s.batchTimer = time.AfterFunc(batchFlushInterval, s.flushBatchAsync)
	}
}
