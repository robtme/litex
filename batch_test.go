package litex

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCommitBatch(t *testing.T) {
	s := openFileDB(t)
	ctx := context.Background()

	mustExec(t, s, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

	if err := s.CommitBatch(ctx, func(b *BatchTX) any { return b }, func(tx any) error {
		b := tx.(*BatchTX)
		if err := b.QueueExec(ctx, "INSERT INTO t (val) VALUES (?)", "a"); err != nil {
			return err
		}

		return b.QueueExec(ctx, "INSERT INTO t (val) VALUES (?)", "b")
	}); err != nil {
		t.Fatalf("CommitBatch: %v", err)
	}

	// Entries are buffered until the flush interval; force the flush for the test.
	if err := s.flushBatch(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	var count int
	if err := s.connRO.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("row count = %d, want 2", count)
	}
}

func TestCommitBatchNotOpen(t *testing.T) {
	s := NewDB(MemoryDSN, testLogger()) // Deliberately not opened.

	if err := s.CommitBatch(context.Background(), func(b *BatchTX) any { return b },
		func(any) error { return nil }); err == nil {
		t.Error("expected error when database not open")
	}
}

func TestBatchQueueArmsTimer(t *testing.T) {
	s := openFileDB(t)
	ctx := context.Background()

	mustExec(t, s, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

	if err := s.CommitBatch(ctx, func(b *BatchTX) any { return b }, func(tx any) error {
		return tx.(*BatchTX).QueueExec(ctx, "INSERT INTO t (val) VALUES ('a')")
	}); err != nil {
		t.Fatal(err)
	}

	// Queueing an entry must arm a flush timer, so buffered writes are never stranded.
	s.batchMu.Lock()
	armed := s.batchTimer != nil
	s.batchMu.Unlock()
	if !armed {
		t.Error("expected a flush timer to be armed after queueing an entry")
	}
}

func TestCommitBatchDiscardsOnCallbackError(t *testing.T) {
	s := openFileDB(t)
	ctx := context.Background()

	mustExec(t, s, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

	// As with CommitWrite, a callback that fails part-way must write nothing: the caller sees an error
	// and is entitled to retry without duplicating the statements that had already been queued.
	wantErr := errors.New("validation failed")

	if err := s.CommitBatch(ctx, func(b *BatchTX) any { return b }, func(tx any) error {
		b := tx.(*BatchTX)
		if err := b.QueueExec(ctx, "INSERT INTO t (val) VALUES (?)", "a"); err != nil {
			return err
		}

		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("CommitBatch err = %v, want %v", err, wantErr)
	}

	s.batchMu.Lock()
	buffered := len(s.batchEntries)
	s.batchMu.Unlock()
	if buffered != 0 {
		t.Errorf("%d entries buffered after a failed callback, want 0", buffered)
	}

	if err := s.flushBatch(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	var count int
	if err := s.connRO.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("row count = %d, want 0", count)
	}
}

func TestFlushBatchRequeuesWhenTxCannotBegin(t *testing.T) {
	s := openFileDB(t)
	ctx := context.Background()

	mustExec(t, s, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

	if err := s.queueBatch(batchEntry{stmt: "INSERT INTO t (val) VALUES (?)", args: []any{"a"}}); err != nil {
		t.Fatal(err)
	}

	// An already-cancelled context fails at BeginTx, before any statement runs. Those entries are
	// recoverable, so they must stay buffered rather than being dropped.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	if err := s.flushBatch(cancelled); err == nil {
		t.Fatal("expected a flush error")
	}

	s.batchMu.Lock()
	buffered := len(s.batchEntries)
	s.batchMu.Unlock()
	if buffered != 1 {
		t.Fatalf("%d entries buffered after a failed begin, want 1", buffered)
	}

	// The retry succeeds and the write lands.
	if err := s.flushBatch(ctx); err != nil {
		t.Fatalf("retry flush: %v", err)
	}

	var count int
	if err := s.connRO.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("row count = %d, want 1", count)
	}
}

func TestFlushBatchRetriesThenDrops(t *testing.T) {
	s := openFileDB(t)
	ctx := context.Background()

	// A statement SQLite refuses every time. It is retried, because the same symptom can come from a
	// transient cause, but it must eventually be dropped: retrying forever would wedge every later
	// flush behind it and grow the buffer without bound.
	if err := s.queueBatch(batchEntry{stmt: "INSERT INTO nope (val) VALUES (?)", args: []any{"a"}}); err != nil {
		t.Fatal(err)
	}

	for attempt := 1; attempt <= maxFlushAttempts; attempt++ {
		if err := s.flushBatch(ctx); err == nil {
			t.Fatalf("attempt %d: expected a flush error", attempt)
		}

		s.batchMu.Lock()
		buffered := len(s.batchEntries)
		s.batchMu.Unlock()

		want := 1
		if attempt == maxFlushAttempts {
			want = 0 // Given up on.
		}

		if buffered != want {
			t.Fatalf("attempt %d: buffered = %d, want %d", attempt, buffered, want)
		}
	}
}

func TestFlushBatchRetriesCommitFailure(t *testing.T) {
	s := openFileDB(t)
	ctx := context.Background()

	// A deferred foreign key passes ExecContext and fails at COMMIT, which is the one path that leaves
	// a flush having run every statement successfully and still applied nothing.
	mustExec(t, s, "CREATE TABLE parent (id INTEGER PRIMARY KEY)")
	mustExec(t, s, `CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER
		REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)`)

	if err := s.queueBatch(batchEntry{stmt: "INSERT INTO child (pid) VALUES (?)", args: []any{999}}); err != nil {
		t.Fatal(err)
	}

	err := s.flushBatch(ctx)
	if err == nil {
		t.Fatal("expected a commit failure")
	}

	if !strings.Contains(err.Error(), "commit batch") {
		t.Fatalf("error = %v, want it to come from the commit", err)
	}

	// Nothing was applied, so the entries must survive for another attempt.
	s.batchMu.Lock()
	buffered := len(s.batchEntries)
	s.batchMu.Unlock()

	if buffered != 1 {
		t.Errorf("buffered = %d after a failed commit, want 1", buffered)
	}

	var count int
	if err := s.connRO.QueryRow("SELECT COUNT(*) FROM child").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("child rows = %d, want 0", count)
	}
}

func TestBatchUnderConcurrentLoad(t *testing.T) {
	s := openFileDB(t)
	ctx := context.Background()

	mustExec(t, s, "CREATE TABLE t (id INTEGER PRIMARY KEY, writer INT, seq INT)")

	// The batch path's real workload: many callers queueing while flushes run underneath them. Every
	// row must land exactly once, so this covers the buffer swap, the cap crossing, requeues, and the
	// final flush in Close all under contention rather than one at a time.
	const (
		numWriters = 16
		perWriter  = 200
	)

	// The flusher gets its own WaitGroup: it runs until the writers are done, so waiting on it in the
	// same group the writers are in would deadlock.
	var writers sync.WaitGroup

	var flusher sync.WaitGroup

	stopFlushing := make(chan struct{})

	// Flush on a much shorter cycle than batchFlushInterval, so the writers race many buffer swaps
	// rather than one. Paced rather than spinning: an unpaced loop commits a transaction per couple of
	// entries, and the fsyncs dominate.

	flusher.Go(func() {

		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stopFlushing:
				return
			case <-ticker.C:
			}

			if err := s.flushBatch(ctx); err != nil {
				t.Errorf("background flush: %v", err)
				return
			}
		}
	})

	for w := range numWriters {

		writers.Go(func() {

			for i := range perWriter {
				if err := s.CommitBatch(ctx, func(b *BatchTX) any { return b }, func(tx any) error {
					return tx.(*BatchTX).QueueExec(ctx,
						"INSERT INTO t (writer, seq) VALUES (?, ?)", w, i)
				}); err != nil {
					t.Errorf("writer %d entry %d: %v", w, i, err)
					return
				}
			}
		})
	}

	// Let the writers finish before stopping the flusher, so nothing is left unqueued.
	writers.Wait()
	close(stopFlushing)
	flusher.Wait()

	// Close performs the final flush, so every queued row is committed by the time it returns.
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	verify := NewDB(s.dsn, testLogger())
	if err := verify.Open(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = verify.Close() }()

	var total, distinct int
	if err := verify.connRO.QueryRow(
		"SELECT COUNT(*), COUNT(DISTINCT writer || ':' || seq) FROM t").Scan(&total, &distinct); err != nil {
		t.Fatal(err)
	}

	if want := numWriters * perWriter; total != want || distinct != want {
		t.Errorf("rows = %d (%d distinct), want %d of each", total, distinct, want)
	}
}

func TestCommitBatchAfterClose(t *testing.T) {
	s := openFileDB(t)
	ctx := context.Background()

	mustExec(t, s, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

	// A request that reaches its CommitBatch just after shutdown. QueueExec only fills the BatchTX's
	// private slice, so the rejection lands when CommitBatch hands those entries to the service.
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := s.CommitBatch(ctx, func(b *BatchTX) any { return b }, func(tx any) error {
		return tx.(*BatchTX).QueueExec(ctx, "INSERT INTO t (val) VALUES ('a')")
	}); err == nil {
		t.Error("expected error committing a batch after Close")
	}

	// Nothing may be left buffered for a flush that can never run, and no timer may outlive Close.
	s.batchMu.Lock()
	stranded, armed := len(s.batchEntries), s.batchTimer != nil
	s.batchMu.Unlock()
	if stranded != 0 || armed {
		t.Errorf("after Close: %d buffered entries, timer armed = %v; want 0, false", stranded, armed)
	}
}

func TestBatchMaxEntriesFlushes(t *testing.T) {
	s := openFileDB(t)

	mustExec(t, s, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

	for range batchMaxEntries {
		if err := s.queueBatch(batchEntry{stmt: "INSERT INTO t (val) VALUES (?)", args: []any{"a"}}); err != nil {
			t.Fatal(err)
		}
	}

	// Hitting the cap pulls the flush forward instead of letting the buffer grow until the next tick.
	// Close waits on the in-flight flush, so every entry must land.
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2 := NewDB(s.dsn, testLogger())
	if err := s2.Open(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	var count int
	if err := s2.connRO.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != batchMaxEntries {
		t.Errorf("row count = %d, want %d", count, batchMaxEntries)
	}
}

func TestFlushBatch(t *testing.T) {
	tests := []struct {
		name         string
		entries      []batchEntry
		wantRows     int
		wantBuffered int
		wantErr      bool
	}{
		{name: "no entries"},
		// A failing statement is re-buffered for another attempt rather than dropped outright, so this
		// one leaves the buffer occupied; TestFlushBatchRetriesThenDrops covers where it ends up.
		{name: "exec error rolls back", entries: []batchEntry{{stmt: "NOT SQL"}}, wantErr: true, wantBuffered: 1},
		{
			name: "success commits every entry",
			entries: []batchEntry{
				{stmt: "INSERT INTO t (val) VALUES (?)", args: []any{"a"}},
				{stmt: "INSERT INTO t (val) VALUES (?)", args: []any{"b"}},
			},
			wantRows: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openFileDB(t)
			mustExec(t, s, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

			s.batchMu.Lock()
			s.batchEntries = tt.entries
			s.batchMu.Unlock()

			if err := s.flushBatch(context.Background()); (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}

			s.batchMu.Lock()
			remaining := len(s.batchEntries)
			s.batchMu.Unlock()
			if remaining != tt.wantBuffered {
				t.Errorf("buffered = %d, want %d", remaining, tt.wantBuffered)
			}

			var count int
			if err := s.connRO.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != tt.wantRows {
				t.Errorf("row count = %d, want %d", count, tt.wantRows)
			}
		})
	}
}

func TestBatchFlushErrorHandler(t *testing.T) {
	s := openFileDB(t)

	var got error
	s.OnBatchFlushError(func(err error) { got = err })

	s.batchMu.Lock()
	s.batchEntries = []batchEntry{{stmt: "NOT SQL"}}
	s.batchMu.Unlock()

	s.flushBatchAsync()

	if got == nil {
		t.Error("expected the flush error to reach the registered handler")
	}
}

func TestBatchFlushSurfacesErrorOnClose(t *testing.T) {
	s := openFileDB(t)
	ctx := context.Background()

	// Queue a statement that will fail at flush time (the table does not exist).
	if err := s.CommitBatch(ctx, func(b *BatchTX) any { return b }, func(tx any) error {
		return tx.(*BatchTX).QueueExec(ctx, "INSERT INTO missing (v) VALUES (1)")
	}); err != nil {
		t.Fatalf("CommitBatch: %v", err)
	}

	if err := s.Close(); err == nil {
		t.Error("expected Close to surface the batch flush error")
	}
}

func TestBatchTXLogger(t *testing.T) {
	s := openFileDB(t)

	called := false
	if err := s.CommitBatch(context.Background(), func(b *BatchTX) any { return b }, func(tx any) error {
		_ = tx.(*BatchTX).Logger()
		called = true

		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Error("batch function was not invoked")
	}
}
