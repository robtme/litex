package litex

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
)

// BaseTX provides common transaction functionality that can be embedded in project-specific transaction types.
type BaseTX struct {
	logger zerolog.Logger
	tx     *sql.Tx
}

// Logger returns the logger for this transaction.
func (b *BaseTX) Logger() *zerolog.Logger {
	return &b.logger
}

// TX returns the underlying sql.Tx for custom operations.
func (b *BaseTX) TX() *sql.Tx {
	return b.tx
}

// Exec executes a write statement against the transaction.
// Prefer this over TX().ExecContext() so that logging is consistent across all write operations.
func (b *BaseTX) Exec(ctx context.Context, stmt string, args ...any) (sql.Result, error) {
	b.logger.Debug().Str("stmt", stmt).Any("args", args).Msg("Executing statement")

	result, err := b.tx.ExecContext(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("exec: %w", err)
	}

	return result, nil
}

// ExecPrepared runs stmt once for each row of args against the transaction, reusing a single
// prepared statement. Use it for bulk writes where the same statement repeats with different values.
func (b *BaseTX) ExecPrepared(ctx context.Context, stmt string, rows [][]any) error {
	b.logger.Debug().Str("stmt", stmt).Int("rows", len(rows)).Msg("Executing prepared statement")

	prepared, err := b.tx.PrepareContext(ctx, stmt)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer prepared.Close()

	for _, args := range rows {
		if _, err = prepared.ExecContext(ctx, args...); err != nil {
			return fmt.Errorf("exec prepared: %w", err)
		}
	}

	return nil
}

// Find is a convenience wrapper for querying rows. The given scanFn is called on each rows.Next() iteration.
func (b *BaseTX) Find(ctx context.Context, stmt string, args []any, scanFn func(rows *sql.Rows) error) error {
	rows, err := b.Query(ctx, stmt, args...)
	if err != nil {
		return fmt.Errorf("find: query table: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			b.logger.Error().Err(closeErr).Msg("Failed to close rows")
		}
	}()

	for rows.Next() {
		if err = scanFn(rows); err != nil {
			return fmt.Errorf("find: fn: %w", err)
		}
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("find: iterating rows: %w", err)
	}

	return nil
}

// Query executes a query against the transaction and returns the resulting rows.
// Prefer this over TX().QueryContext() so that logging is consistent across all read operations.
func (b *BaseTX) Query(ctx context.Context, stmt string, args ...any) (*sql.Rows, error) {
	b.logger.Debug().Str("stmt", stmt).Any("args", args).Msg("Querying table")

	rows, err := b.tx.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	return rows, nil
}

// QueryRow executes a query against the transaction and returns a single row.
// Prefer this over TX().QueryRow() so that logging is consistent across all read operations.
func (b *BaseTX) QueryRow(ctx context.Context, stmt string, args ...any) *sql.Row {
	b.logger.Debug().Str("stmt", stmt).Any("args", args).Msg("Querying table for row")

	return b.tx.QueryRowContext(ctx, stmt, args...)
}

// CommitRead opens a read-only database transaction.
// The newTx function converts a BaseTX into your project-specific transaction type.
func (s *Service) CommitRead(ctx context.Context, newTx func(*BaseTX) any, fn func(any) error) error {
	if s.connRO == nil {
		return errors.New("database not open")
	}

	tx, err := s.connRO.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("commit read: begin: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			s.logger.Error().Err(rollbackErr).Msg("Failed to rollback read-only transaction")
		}
	}()

	if err = fn(newTx(&BaseTX{logger: *s.logger, tx: tx})); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit read: end: %w", err)
	}

	return nil
}

// CommitWrite opens a read-write database transaction.
// The newTx function converts a BaseTX into your project-specific transaction type.
func (s *Service) CommitWrite(ctx context.Context, newTx func(*BaseTX) any, fn func(any) error) error {
	if s.connRW == nil {
		return errors.New("database not open")
	}

	tx, err := s.connRW.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("commit write: begin: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			s.logger.Error().Err(rollbackErr).Msg("Failed to rollback read-write transaction")
		}
	}()

	if err = fn(newTx(&BaseTX{logger: *s.logger, tx: tx})); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit write: end: %w", err)
	}

	return nil
}
