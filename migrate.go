package litex

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	// maxForeignKeyViolationSamples caps how many foreign key violations are included in the error
	// surfaced by checkForeignKeys, to aid debugging without flooding the message.
	maxForeignKeyViolationSamples = 10

	// maxUserVersion is the largest value SQLite's PRAGMA user_version can hold; it is a signed
	// 32-bit integer stored in the database header.
	maxUserVersion = 1<<31 - 1
)

// Migrate applies, in ascending order, every migration newer than the database's PRAGMA user_version,
// then records the highest applied. Call it after Open.
//
// Files live under migrations/ and are named <version>.sql with an optional _description (1.sql,
// 1_init.sql, 20260716_add_users.sql). Versions need not be contiguous. Migrations are forward-only
// and may not exceed 2147483647, the user_version ceiling (YYYYMMDD fits; a full timestamp does not).
func (s *Service) Migrate(ctx context.Context, migrations fs.FS) error {
	if s.connRW == nil {
		return errors.New("database not open")
	}

	// The pinned connection is fully owned by runMigrations, so it is freed before the VACUUM below.
	// The RW pool is capped at one connection, so holding it across VACUUM would deadlock.
	migrationsRan, err := s.runMigrations(ctx, migrations)
	if err != nil {
		return err
	}

	if migrationsRan {
		if !s.memory {
			s.vacuum(ctx)
		}

		// Schema changes (new indexes especially) invalidate sqlite_stat1.
		s.optimize()
	}

	return nil
}

// vacuum reclaims space left by the migrations that just ran. Failures are logged rather than
// returned, since failing here would turn a full disk into a service that refuses to start. VACUUM
// rebuilds the database into a copy needing roughly its own size free, written to SQLite's temp
// directory rather than next to the database, so both are checked for space.
func (s *Service) vacuum(ctx context.Context) {
	dbInfo, err := os.Stat(s.dsn)
	if err != nil {
		s.logger.Warn().Err(err).Msg("Skipping database cleanup: cannot stat database file")
		return
	}

	const requiredSpaceMultiplier = 2.0

	required := uint64(float64(dbInfo.Size()) * requiredSpaceMultiplier)

	available := min(getAvailableDiskSpace(filepath.Dir(s.dsn)), getAvailableDiskSpace(tempDir()))
	if required >= available {
		s.logger.Warn().Uint64("requiredSpace", required).Uint64("availableSpace", available).
			Msg("Skipping database cleanup: not enough disk space")

		return
	}

	s.logger.Info().Msg("Running SQL cleanup (this may take a while)")

	if _, err = s.connRW.ExecContext(ctx, `VACUUM;`); err != nil {
		s.logger.Warn().Err(err).Msg("Database cleanup failed")
	}
}

// tempDir reports where SQLite writes VACUUM's rebuild copy. SQLITE_TMPDIR takes precedence
// over the platform temp directory that os.TempDir already resolves from TMPDIR.
func tempDir() string {
	if dir := os.Getenv("SQLITE_TMPDIR"); dir != "" {
		return dir
	}

	return os.TempDir()
}

// runMigrations applies pending migrations within a single pinned connection, returning whether any
// ran. Foreign key enforcement is disabled before BEGIN (the pragma is connection-scoped and a no-op
// inside a transaction) so table-rebuild migrations don't cascade-delete child rows.
func (s *Service) runMigrations(ctx context.Context, migrations fs.FS) (bool, error) {
	available, err := loadMigrations(migrations)
	if err != nil {
		return false, err
	}

	// A mistyped embed pattern or an FS already rooted at migrations/ globs to nothing, and starting
	// against an empty schema only surfaces much later as "no such table".
	if len(available) == 0 {
		return false, errors.New("no migrations found: expected migrations/*.sql in the given fs.FS")
	}

	conn, err := s.connRW.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("migrate: acquire connection: %w", err)
	}
	defer func() {
		// Restore FK enforcement before the connection returns to the pool, otherwise a later borrower
		// would silently run with FKs disabled. The context is detached because a canceled ctx (which
		// may be what aborted the migration) would skip the restore.
		restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pingTimeout)
		defer cancel()

		if _, restoreErr := conn.ExecContext(restoreCtx, "PRAGMA foreign_keys = ON"); restoreErr != nil {
			s.logger.Error().Err(restoreErr).Msg("Failed to restore foreign_keys pragma after migration")
		}
		if closeErr := conn.Close(); closeErr != nil {
			s.logger.Error().Err(closeErr).Msg("Failed to close migration connection")
		}
	}()

	if _, err = conn.ExecContext(ctx, "PRAGMA foreign_keys = OFF"); err != nil {
		return false, fmt.Errorf("migrate: disable foreign_keys: %w", err)
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("migrate: begin: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			s.logger.Error().Err(rollbackErr).Msg("Failed to rollback migration transaction")
		}
	}()

	var schemaVersion int

	row := tx.QueryRowContext(ctx, "PRAGMA user_version")
	if err = row.Scan(&schemaVersion); err != nil {
		return false, fmt.Errorf("check schema version: %w", err)
	}

	// A database past the newest migration this build ships means the build is older than the
	// database; refuse rather than run against a schema it does not recognize. A missing file for the
	// current version is fine on its own: that is what a squashed baseline looks like.
	if newest := available[len(available)-1].version; schemaVersion > newest {
		return false, fmt.Errorf("database schema version %d is newer than the latest available migration %d; this build is older than the database", schemaVersion, newest)
	}

	pending := make([]migrationFile, 0, len(available))
	for _, m := range available {
		if m.version > schemaVersion {
			pending = append(pending, m)
		}
	}

	if len(pending) == 0 {
		return false, nil
	}

	// Sorted ascending, so the last entry is the newest.
	latestVersion := pending[len(pending)-1].version

	s.logger.Info().Msgf("Running SQL migrations from version %d to %d", schemaVersion, latestVersion)

	for _, m := range pending {
		contents, err := fs.ReadFile(migrations, m.path)
		if err != nil {
			return false, fmt.Errorf("read migration %s: %w", m.path, err)
		}

		s.logger.Info().Msgf("Applying SQL migration: %d", m.version)

		if _, err = tx.ExecContext(ctx, string(contents)); err != nil {
			return false, fmt.Errorf("apply migration %d: %w", m.version, err)
		}
	}

	// With FK enforcement disabled the migrations above could have introduced dangling
	// references. Verify integrity before committing so a broken migration fails loudly
	// instead of silently persisting orphaned rows.
	if err = checkForeignKeys(ctx, tx); err != nil {
		return false, err
	}

	if _, err = tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", latestVersion)); err != nil {
		return false, fmt.Errorf("update schema version: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return false, fmt.Errorf("migrate: commit: %w", err)
	}

	return true, nil
}

// migrationFile is a discovered migration: its parsed integer version and its path within the FS.
type migrationFile struct {
	version int
	path    string
}

// loadMigrations finds migrations/*.sql in the given FS, parses each file's version, rejects
// duplicate versions, and returns them sorted by version ascending.
func loadMigrations(migrations fs.FS) ([]migrationFile, error) {
	files, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return nil, fmt.Errorf("list migrations: %w", err)
	}

	found := make([]migrationFile, 0, len(files))
	seen := make(map[int]string, len(files))

	for _, file := range files {
		version, err := parseMigrationVersion(file)
		if err != nil {
			return nil, err
		}

		if existing, dup := seen[version]; dup {
			return nil, fmt.Errorf("duplicate migration version %d: %q and %q", version, existing, file)
		}

		seen[version] = file
		found = append(found, migrationFile{version: version, path: file})
	}

	sort.Slice(found, func(i, j int) bool { return found[i].version < found[j].version })

	return found, nil
}

// parseMigrationVersion extracts the leading integer version from a migration filename. Names must
// be <version>.sql or <version>_<description>.sql, where version is a positive integer that fits in
// PRAGMA user_version.
func parseMigrationVersion(file string) (int, error) {
	name := strings.TrimSuffix(path.Base(file), ".sql")

	digits := name
	if before, _, ok := strings.Cut(name, "_"); ok {
		digits = before
	}

	version, err := strconv.Atoi(digits)
	if err != nil {
		return 0, fmt.Errorf("migration %q: name must start with an integer version (e.g. 1.sql or 20260716_add_users.sql)", file)
	}

	if version <= 0 {
		return 0, fmt.Errorf("migration %q: version must be a positive integer", file)
	}

	if version > maxUserVersion {
		return 0, fmt.Errorf("migration %q: version %d exceeds the maximum of %d (user_version is a 32-bit integer)", file, version, maxUserVersion)
	}

	return version, nil
}

// checkForeignKeys runs PRAGMA foreign_key_check against the transaction and returns an error if any
// violations exist, sampling the first few to aid debugging without flooding the message.
func checkForeignKeys(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("foreign key check: %w", err)
	}
	defer rows.Close()

	var count int
	var violations []string

	for rows.Next() {
		count++

		// Columns: table, rowid, referred-table, fkid. rowid may be NULL for WITHOUT ROWID tables.
		var fkID int
		var referredTbl string
		var rowID sql.NullInt64
		var table string

		if err = rows.Scan(&table, &rowID, &referredTbl, &fkID); err != nil {
			return fmt.Errorf("foreign key check: scan violation: %w", err)
		}

		if len(violations) < maxForeignKeyViolationSamples {
			rowIDStr := "NULL"
			if rowID.Valid {
				rowIDStr = strconv.FormatInt(rowID.Int64, 10)
			}

			violations = append(violations, fmt.Sprintf("%s(rowid=%s) -> %s", table, rowIDStr, referredTbl))
		}
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("foreign key check: iterate: %w", err)
	}

	if count > 0 {
		return fmt.Errorf("migration produced %d foreign key violation(s): %s", count, strings.Join(violations, ", "))
	}

	return nil
}
