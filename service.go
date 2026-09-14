// Package litex provides shared SQLite utilities including connection setup,
// transaction management, and query building helpers.
package litex

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	_ "modernc.org/sqlite" // Imports SQLite driver.
)

const (
	// MemoryDSN is used to define an in-memory-only database.
	MemoryDSN = ":memory:"

	// TimeFormat is the default time format for SQLite timestamps.
	// Source: https://pkg.go.dev/modernc.org/sqlite#Driver.Open.
	TimeFormat = "2006-01-02 15:04:05.999 -0700 MST"

	// idleConnectionPoolDivider is the amount the total connection pool will be divided by for the idle connection
	// pool.
	idleConnectionPoolDivider = 2

	// optimizeTimeout bounds PRAGMA optimize, which may run ANALYZE on several tables.
	optimizeTimeout = 30 * time.Second

	// pingTimeout enforces a timeout for the initial connection ping.
	pingTimeout = 5 * time.Second

	// readOnlyConnectionPool is the default number of read-only connections.
	readOnlyConnectionPool = 32

	// readWriteConnectionPool is the number of read-write connections.
	// Do not change this - SQLite only supports one writer at a time.
	readWriteConnectionPool = 1
)

// memorySeq disambiguates in-memory database names so two services created in the same nanosecond do
// not accidentally share a database.
var memorySeq atomic.Uint64

// Service represents a SQLite database service.
type Service struct {
	// Assigned once by Open and never written again, which is what lets every entry point read them
	// unsynchronized. Close closes the handles but leaves the fields set, so a caller racing Close gets
	// sql.ErrDBClosed rather than a nil dereference; nil means "never opened".
	connRO *sql.DB
	connRW *sql.DB

	dsn    string
	logger *zerolog.Logger
	memory bool

	batchMu         sync.Mutex
	batchEntries    []batchEntry
	batchTimer      *time.Timer
	batchErrHandler func(error)
	batchAttempts   int
	closed          bool

	// connMu serializes batch flushes against each other and against Close, so that Close waits for a
	// flush the timer already started rather than closing the connections it is midway through using.
	// Lock ordering is connMu then batchMu, never the reverse.
	connMu sync.Mutex

	// Repeated and concurrent Close calls wait on this and share closeErr.
	closeOnce sync.Once
	closeErr  error
}

// NewDB creates a new SQLite database service.
func NewDB(dsn string, logger zerolog.Logger) *Service {
	if dsn == "" {
		panic("sqlite: dsn is required")
	}
	db := &Service{
		logger: new(logger.With().Str("layer", "sqlite").Logger()),
	}

	if dsn == MemoryDSN {
		// Unique in-memory db name shared between the RO/RW connections, salted with a sequence number
		// so concurrently created services never collide on the shared-cache name.
		db.dsn = fmt.Sprintf("litex%d_%d", time.Now().UnixNano(), memorySeq.Add(1))
		db.memory = true
	} else {
		db.dsn = dsn
	}

	return db
}

// Close flushes any buffered batch writes and closes the connections, joining any flush failure with
// the close errors so shutdown data loss is visible. Closing twice is a no-op, and a Service is
// single-use: construct a new one with NewDB rather than reopening. It blocks for up to twice
// batchFlushTimeout, since the final flush waits out one the timer had already started.
func (s *Service) Close() error {
	// Never opened: nothing to flush or close. Checked before the once so a Service closed in this
	// state can still be opened and closed properly later.
	if s.connRW == nil {
		return nil
	}

	s.closeOnce.Do(func() {
		// Reject further batch entries before flushing, so nothing can be queued (and have a flush
		// timer armed) behind the final flush.
		s.batchMu.Lock()
		s.closed = true
		s.batchMu.Unlock()

		// Acquired before the clock starts, so the final flush gets the whole timeout rather than what
		// an in-flight flush left of it.
		s.connMu.Lock()
		defer s.connMu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), batchFlushTimeout)
		defer cancel()

		var errs []error

		if err := s.flushLocked(ctx); err != nil {
			errs = append(errs, fmt.Errorf("flush batch: %w", err))
		}

		s.optimize()

		// The fields keep pointing at the closed handles: clearing them would turn a caller mid-call
		// into a nil dereference.
		if err := s.connRO.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close read-only conn: %w", err))
		}

		if err := s.connRW.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close read-write conn: %w", err))
		}

		s.closeErr = errors.Join(errs...)
	})

	return s.closeErr
}

// Open opens the database connections. Call Migrate separately afterwards, and call Open itself once,
// before sharing the Service: it is the only writer of the connection fields, which is what lets
// every other method read them unsynchronized.
func (s *Service) Open() error {
	if s.dsn == "" {
		return errors.New("missing dsn")
	}
	if s.connRW != nil {
		return errors.New("already open")
	}

	// Make the parent directory unless using an in-memory db.
	if !s.memory {
		if err := os.MkdirAll(filepath.Dir(s.dsn), 0o700); err != nil {
			return fmt.Errorf("create database path: %w", err)
		}
	}

	// Read-write connection must be opened first to ensure journal mode consistency for both.
	connRW, err := setupConn(s.dsn, s.memory, false)
	if err != nil {
		return fmt.Errorf("setup read-write database connection: %w", err)
	}

	connRO, err := setupConn(s.dsn, s.memory, true)
	if err != nil {
		_ = connRW.Close()
		return fmt.Errorf("setup read-only database connection: %w", err)
	}

	s.connRO, s.connRW = connRO, connRW

	return nil
}

// optimize runs PRAGMA optimize on the read-write connection, letting SQLite re-ANALYZE tables whose
// statistics have gone stale. Recommended after schema changes and before closing a long-lived
// connection. Failures are logged rather than returned; stale query plans are not worth failing a
// shutdown or a startup over.
func (s *Service) optimize() {
	ctx, cancel := context.WithTimeout(context.Background(), optimizeTimeout)
	defer cancel()

	if _, err := s.connRW.ExecContext(ctx, "PRAGMA optimize"); err != nil {
		s.logger.Warn().Err(err).Msg("Database optimize failed")
	}
}

// setupConn initializes a SQLite database connection with configured PRAGMA settings and connection pooling. We set
// pragmas at the DSN-level, as executing them in a query does not guarantee they'll be used for all subsequent
// connections.
func setupConn(dsn string, memory bool, readOnly bool) (*sql.DB, error) {
	mode := "rwc"
	if readOnly {
		mode = "ro"
	}

	var formattedDSN string

	if !memory {
		formattedDSN = "file:" + dsn + "?_time_format=sqlite&_timezone=UTC&mode=" + mode
	} else {
		// Share the in-memory database so the separate RO/RW connections use the same DB.
		formattedDSN = "file:" + dsn + "?mode=memory&cache=shared&_time_format=sqlite&_timezone=UTC"
	}

	var idleConnectionPool, maxConnectionPool int

	pragmas := []string{
		"busy_timeout(5000)",   // Wait up to 5s on locked database before returning an error.
		"mmap_size(134217728)", // Enable memory-mapped I/O (~128MB) to improve read performance.
	}

	// txlock controls how BeginTx acquires its lock. The read-write connection uses BEGIN IMMEDIATE so the write lock is
	// taken up front rather than lazily on upgrade. Without this, a deferred transaction that reads before it writes
	// takes a stale read snapshot and fails with SQLITE_BUSY_SNAPSHOT (517) if another connection commits in between -
	// an error busy_timeout does NOT retry. With BEGIN IMMEDIATE, contention degrades to plain SQLITE_BUSY, which
	// busy_timeout waits on. The read-only pool stays deferred (the driver only applies the lock mode when !ReadOnly).
	txlock := ""

	if readOnly {
		idleConnectionPool = getMin(readOnlyConnectionPool / idleConnectionPoolDivider)
		maxConnectionPool = readOnlyConnectionPool

		pragmas = append(
			pragmas,
			"cache_size(500)", // Page cache size per connection (smaller to limit total memory across many RO conns).
			"query_only(1)",   // Enforces read-only mode at the connection level (blocks writes even if attempted).
		)
	} else {
		idleConnectionPool = getMin(readWriteConnectionPool / idleConnectionPoolDivider)
		maxConnectionPool = readWriteConnectionPool

		pragmas = append(
			pragmas,
			"cache_size(2000)",             // Larger page cache for the read-write connection to improve write/query efficiency.
			"foreign_keys(1)",              // Enforce foreign key constraints on writes.
			"journal_size_limit(67108864)", // Caps WAL file size (~64MB) before it is truncated during checkpoints.
			"optimize(0x10002)",            // Re-ANALYZE tables whose row counts drifted since the last run; recommended once per long-lived connection.
			"synchronous(NORMAL)",          // Relax fsync frequency for better performance with acceptable durability tradeoff (safe with WAL).
		)

		txlock = "&_txlock=immediate"

		// WAL is only needed for file databases.
		if !memory {
			pragmas = append(
				pragmas,
				"journal_mode(WAL)", // Enables WAL mode: allows concurrent readers + single writer.
			)
		}
	}

	conn, err := sql.Open("sqlite", formattedDSN+"&_pragma="+strings.Join(pragmas, "&_pragma=")+txlock)
	if err != nil {
		return nil, fmt.Errorf("open sql connection: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()

	// Ping connection to ensure connectivity, and that all PRAGMAs have been applied.
	if err = conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping sql connection: %w", err)
	}

	conn.SetMaxIdleConns(idleConnectionPool) // Keep half of the total connections warm without holding the full pool.
	conn.SetMaxOpenConns(maxConnectionPool)  // Hard cap on concurrent connections.

	return conn, nil
}

func getMin(val int) int {
	if val < 1 {
		return 1
	}

	return val
}
