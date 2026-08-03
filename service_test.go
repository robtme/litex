package litex

import (
	"context"
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/rs/zerolog"
)

// testLogger returns a logger that discards output so tests stay quiet.
func testLogger() zerolog.Logger {
	return zerolog.New(io.Discard)
}

// openMemoryDB returns an opened in-memory Service and registers its cleanup.
func openMemoryDB(t *testing.T) *Service {
	t.Helper()

	s := NewDB(MemoryDSN, testLogger())
	if err := s.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	return s
}

// openFileDB returns an opened file-backed Service in a temp directory and registers its cleanup.
func openFileDB(t *testing.T) *Service {
	t.Helper()

	s := NewDB(filepath.Join(t.TempDir(), "test.db"), testLogger())
	if err := s.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	return s
}

func TestNewDB(t *testing.T) {
	tests := []struct {
		name        string
		dsn         string
		wantMemory  bool
		wantPanic   bool
		wantSameDSN bool
	}{
		{name: "memory dsn", dsn: MemoryDSN, wantMemory: true},
		{name: "file dsn", dsn: "data/app.db", wantSameDSN: true},
		{name: "empty dsn panics", dsn: "", wantPanic: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantPanic {
				defer func() {
					if r := recover(); r == nil {
						t.Error("expected panic for empty dsn")
					}
				}()

				_ = NewDB(tt.dsn, testLogger())

				return
			}

			s := NewDB(tt.dsn, testLogger())

			if s.memory != tt.wantMemory {
				t.Errorf("memory = %v, want %v", s.memory, tt.wantMemory)
			}

			if tt.wantSameDSN && s.dsn != tt.dsn {
				t.Errorf("dsn = %q, want %q", s.dsn, tt.dsn)
			}

			if tt.wantMemory && s.dsn == MemoryDSN {
				t.Error("expected a unique in-memory dsn, got the MemoryDSN literal")
			}
		})
	}
}

func TestOpenClose(t *testing.T) {
	tests := []struct {
		name    string
		dsn     func(t *testing.T) string
		wantErr bool
	}{
		{
			name: "in memory",
			dsn:  func(*testing.T) string { return MemoryDSN },
		},
		{
			name: "file based",
			dsn:  func(t *testing.T) string { return filepath.Join(t.TempDir(), "test.db") },
		},
		{
			name: "parent dir is a file",
			dsn: func(t *testing.T) string {
				f, err := os.CreateTemp(t.TempDir(), "notdir-*")
				if err != nil {
					t.Fatal(err)
				}
				_ = f.Close()

				return filepath.Join(f.Name(), "test.db")
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewDB(tt.dsn(t), testLogger())

			err := s.Open()
			if tt.wantErr {
				if err == nil {
					t.Error("expected Open error")
				}

				return
			}
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer s.Close()

			if s.connRO == nil || s.connRW == nil {
				t.Error("connections not initialized")
			}
		})
	}
}

func TestOpenMissingDSN(t *testing.T) {
	// A Service constructed without a dsn (bypassing NewDB) must refuse to open.
	s := &Service{}
	if err := s.Open(); err == nil {
		t.Error("expected error for missing dsn")
	}
}

func TestOpenTwice(t *testing.T) {
	s := openMemoryDB(t) // Already open, with cleanup registered.

	if err := s.Open(); err == nil {
		t.Error("expected error when opening an already-open service")
	}
}

func TestWriteAfterClose(t *testing.T) {
	s := NewDB(filepath.Join(t.TempDir(), "test.db"), testLogger())
	if err := s.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A Service is single-use, so a write after shutdown errors rather than reaching a live database.
	if err := s.CommitWrite(context.Background(), func(b *BaseTX) any { return b }, func(any) error { return nil }); err == nil {
		t.Error("expected error writing to a closed service")
	}

	if err := s.Open(); err == nil {
		t.Error("expected error reopening a closed service")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	// A deferred Close paired with an explicit shutdown Close, and a deferred Close after a failed
	// Open, are both ordinary patterns; neither may report a spurious error.
	s := NewDB(filepath.Join(t.TempDir(), "test.db"), testLogger())
	if err := s.Open(); err != nil {
		t.Fatalf("open: %v", err)
	}

	for i := range 3 {
		if err := s.Close(); err != nil {
			t.Errorf("close %d: %v", i+1, err)
		}
	}

	if err := NewDB(filepath.Join(t.TempDir(), "never.db"), testLogger()).Close(); err != nil {
		t.Errorf("close without open: %v", err)
	}
}

func TestConcurrentQueueExec(t *testing.T) {
	s := openFileDB(t)
	ctx := context.Background()

	mustExec(t, s, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

	// A callback may fan QueueExec out across goroutines, so every statement must survive.
	const writers = 8

	const perWriter = 50

	if err := s.CommitBatch(ctx, func(b *BatchTX) any { return b }, func(tx any) error {
		b := tx.(*BatchTX)

		var wg sync.WaitGroup
		for range writers {
			wg.Go(func() {
				for range perWriter {
					_ = b.QueueExec(ctx, "INSERT INTO t (val) VALUES (?)", "x")
				}
			})
		}

		wg.Wait()

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := s.flushBatch(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	var count int
	if err := s.connRO.QueryRow("SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != writers*perWriter {
		t.Errorf("row count = %d, want %d", count, writers*perWriter)
	}
}

func TestConcurrentCloseAndWrite(t *testing.T) {
	s := openFileDB(t)
	mustExec(t, s, "CREATE TABLE t (id INTEGER PRIMARY KEY)")

	ctx := context.Background()
	newTx := func(b *BaseTX) any { return b }
	write := func(tx any) error {
		_, err := tx.(*BaseTX).Exec(ctx, "INSERT INTO t DEFAULT VALUES")
		return err
	}

	// Callers racing Close must get an error, never a nil-pointer dereference: Close must not leave a
	// window where an entry point has passed its open check but the handle it dereferences is gone.
	var wg sync.WaitGroup

	running := make(chan struct{}, 8)
	stop := make(chan struct{})

	for range 8 {
		wg.Go(func() {
			for first := true; ; first = false {
				select {
				case <-stop:
					return
				default:
				}

				_ = s.CommitWrite(ctx, newTx, write)
				_ = s.CommitRead(ctx, newTx, func(any) error { return nil })

				if first {
					running <- struct{}{}
				}
			}
		})
	}

	// Only close once every goroutine is inside the loop, otherwise Close finishes first and the window
	// this test exists to cover is never reached.
	for range 8 {
		<-running
	}

	if err := s.Close(); err != nil {
		t.Errorf("close: %v", err)
	}

	close(stop)
	wg.Wait()

	// The invariant that makes the above safe, asserted directly because the race window itself is too
	// narrow to hit reliably: Close must leave both handles set, so a caller that has already passed
	// its nil check dereferences a live (closed) *sql.DB and gets an error instead of panicking.
	if s.connRO == nil || s.connRW == nil {
		t.Fatal("Close cleared a connection handle; a caller past its nil check would dereference nil")
	}

	if _, err := s.connRW.ExecContext(ctx, "INSERT INTO t DEFAULT VALUES"); err == nil {
		t.Error("expected an error from the closed read-write handle")
	}
}

func TestMemoryDSNUnique(t *testing.T) {
	a := NewDB(MemoryDSN, testLogger())
	b := NewDB(MemoryDSN, testLogger())

	if a.dsn == b.dsn {
		t.Errorf("expected unique in-memory dsns, both are %q", a.dsn)
	}
}

func TestGetMin(t *testing.T) {
	tests := []struct {
		in   int
		want int
	}{
		{in: -5, want: 1},
		{in: 0, want: 1},
		{in: 1, want: 1},
		{in: 16, want: 16},
	}

	for _, tt := range tests {
		if got := getMin(tt.in); got != tt.want {
			t.Errorf("getMin(%d) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestSetupConnPragmas(t *testing.T) {
	s := openFileDB(t)

	intPragmas := []struct {
		name   string
		db     *sql.DB
		pragma string
		want   int
	}{
		{name: "read-write enforces foreign keys", db: s.connRW, pragma: "PRAGMA foreign_keys", want: 1},
		{name: "read-only is query_only", db: s.connRO, pragma: "PRAGMA query_only", want: 1},
	}

	for _, tt := range intPragmas {
		t.Run(tt.name, func(t *testing.T) {
			var got int
			if err := tt.db.QueryRow(tt.pragma).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("%s = %d, want %d", tt.pragma, got, tt.want)
			}
		})
	}

	t.Run("read-write uses WAL", func(t *testing.T) {
		var mode string
		if err := s.connRW.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
			t.Fatal(err)
		}
		if mode != "wal" {
			t.Errorf("journal_mode = %q, want wal", mode)
		}
	})
}

func TestMemorySharedConnections(t *testing.T) {
	s := openMemoryDB(t)

	// The read-only and read-write connections must share the same in-memory database.
	if _, err := s.connRW.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.connRW.Exec("INSERT INTO t (v) VALUES ('shared')"); err != nil {
		t.Fatal(err)
	}

	var v string
	if err := s.connRO.QueryRow("SELECT v FROM t").Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != "shared" {
		t.Errorf("value = %q, want shared", v)
	}
}
