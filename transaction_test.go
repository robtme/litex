package litex

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

// passBaseTX is a no-op newTx adapter that hands the *BaseTX straight to the work function.
func passBaseTX(b *BaseTX) any { return b }

// mustExec runs a single write statement in its own transaction, failing the test on error.
func mustExec(t *testing.T, s *Service, stmt string, args ...any) {
	t.Helper()

	ctx := context.Background()
	if err := s.CommitWrite(ctx, passBaseTX, func(tx any) error {
		_, err := tx.(*BaseTX).Exec(ctx, stmt, args...)
		return err
	}); err != nil {
		t.Fatalf("exec %q: %v", stmt, err)
	}
}

func TestCommitNotOpen(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Service) error
	}{
		{
			name: "commit read",
			run: func(s *Service) error {
				return s.CommitRead(context.Background(), passBaseTX, func(any) error { return nil })
			},
		},
		{
			name: "commit write",
			run: func(s *Service) error {
				return s.CommitWrite(context.Background(), passBaseTX, func(any) error { return nil })
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewDB(MemoryDSN, testLogger()) // Deliberately not opened.
			if err := tt.run(s); err == nil {
				t.Error("expected error when database not open")
			}
		})
	}
}

func TestCommitRoundTrip(t *testing.T) {
	s := openMemoryDB(t)
	ctx := context.Background()

	mustExec(t, s, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	mustExec(t, s, "INSERT INTO t (v) VALUES (?)", "hello")

	var v string
	if err := s.CommitRead(ctx, passBaseTX, func(tx any) error {
		return tx.(*BaseTX).QueryRow(ctx, "SELECT v FROM t").Scan(&v)
	}); err != nil {
		t.Fatal(err)
	}
	if v != "hello" {
		t.Errorf("value = %q, want hello", v)
	}
}

func TestCommitWriteRollback(t *testing.T) {
	s := openMemoryDB(t)
	ctx := context.Background()

	mustExec(t, s, "CREATE TABLE t (id INTEGER PRIMARY KEY)")

	sentinel := errors.New("boom")
	if err := s.CommitWrite(ctx, passBaseTX, func(tx any) error {
		if _, execErr := tx.(*BaseTX).Exec(ctx, "INSERT INTO t (id) VALUES (1)"); execErr != nil {
			return execErr
		}

		return sentinel // Returning an error must roll back the insert above.
	}); !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want sentinel", err)
	}

	var count int
	if err := s.CommitRead(ctx, passBaseTX, func(tx any) error {
		return tx.(*BaseTX).QueryRow(ctx, "SELECT COUNT(*) FROM t").Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("row count = %d, want 0 (rolled back)", count)
	}
}

func TestBaseTXExec(t *testing.T) {
	s := openMemoryDB(t)
	ctx := context.Background()

	mustExec(t, s, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")

	tests := []struct {
		name    string
		stmt    string
		args    []any
		wantErr bool
	}{
		{name: "valid insert", stmt: "INSERT INTO t (name) VALUES (?)", args: []any{"a"}},
		{name: "invalid sql", stmt: "NOT SQL", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.CommitWrite(ctx, passBaseTX, func(tx any) error {
				_, execErr := tx.(*BaseTX).Exec(ctx, tt.stmt, tt.args...)
				return execErr
			})
			if tt.wantErr && err == nil {
				t.Error("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestBaseTXExecPrepared(t *testing.T) {
	s := openMemoryDB(t)
	ctx := context.Background()

	mustExec(t, s, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")

	t.Run("bulk insert", func(t *testing.T) {
		if err := s.CommitWrite(ctx, passBaseTX, func(tx any) error {
			return tx.(*BaseTX).ExecPrepared(ctx, "INSERT INTO t (name) VALUES (?)", [][]any{{"a"}, {"b"}, {"c"}})
		}); err != nil {
			t.Fatal(err)
		}

		var count int
		if err := s.CommitRead(ctx, passBaseTX, func(tx any) error {
			return tx.(*BaseTX).QueryRow(ctx, "SELECT COUNT(*) FROM t").Scan(&count)
		}); err != nil {
			t.Fatal(err)
		}
		if count != 3 {
			t.Errorf("row count = %d, want 3", count)
		}
	})

	t.Run("invalid statement", func(t *testing.T) {
		if err := s.CommitWrite(ctx, passBaseTX, func(tx any) error {
			return tx.(*BaseTX).ExecPrepared(ctx, "NOT SQL", [][]any{{"a"}})
		}); err == nil {
			t.Error("expected error")
		}
	})
}

func TestBaseTXFind(t *testing.T) {
	s := openMemoryDB(t)
	ctx := context.Background()

	mustExec(t, s, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, s, "INSERT INTO t (name) VALUES (?), (?)", "a", "b")

	t.Run("scans every row", func(t *testing.T) {
		var names []string
		if err := s.CommitRead(ctx, passBaseTX, func(tx any) error {
			return tx.(*BaseTX).Find(ctx, "SELECT name FROM t ORDER BY id", nil, func(rows *sql.Rows) error {
				var name string
				if err := rows.Scan(&name); err != nil {
					return err
				}
				names = append(names, name)

				return nil
			})
		}); err != nil {
			t.Fatal(err)
		}
		if len(names) != 2 || names[0] != "a" || names[1] != "b" {
			t.Errorf("names = %v, want [a b]", names)
		}
	})

	t.Run("scan error propagates", func(t *testing.T) {
		sentinel := errors.New("scan boom")
		if err := s.CommitRead(ctx, passBaseTX, func(tx any) error {
			return tx.(*BaseTX).Find(ctx, "SELECT name FROM t", nil, func(*sql.Rows) error { return sentinel })
		}); !errors.Is(err, sentinel) {
			t.Errorf("error = %v, want sentinel", err)
		}
	})

	t.Run("query error", func(t *testing.T) {
		if err := s.CommitRead(ctx, passBaseTX, func(tx any) error {
			return tx.(*BaseTX).Find(ctx, "NOT SQL", nil, func(*sql.Rows) error { return nil })
		}); err == nil {
			t.Error("expected error")
		}
	})
}

func TestBaseTXQuery(t *testing.T) {
	s := openMemoryDB(t)
	ctx := context.Background()

	mustExec(t, s, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, s, "INSERT INTO t (name) VALUES ('x')")

	t.Run("query and query row", func(t *testing.T) {
		if err := s.CommitRead(ctx, passBaseTX, func(tx any) error {
			b := tx.(*BaseTX)

			rows, err := b.Query(ctx, "SELECT name FROM t")
			if err != nil {
				return err
			}
			defer rows.Close()

			if !rows.Next() {
				t.Error("expected at least one row")
			}
			if err := rows.Err(); err != nil {
				return err
			}

			var name string
			if err := b.QueryRow(ctx, "SELECT name FROM t").Scan(&name); err != nil {
				return err
			}
			if name != "x" {
				t.Errorf("name = %q, want x", name)
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("query error", func(t *testing.T) {
		if err := s.CommitRead(ctx, passBaseTX, func(tx any) error {
			rows, queryErr := tx.(*BaseTX).Query(ctx, "NOT SQL")
			if queryErr != nil {
				return queryErr
			}
			defer rows.Close()

			return rows.Err()
		}); err == nil {
			t.Error("expected error")
		}
	})
}

func TestBaseTXAccessors(t *testing.T) {
	s := openMemoryDB(t)

	if err := s.CommitRead(context.Background(), passBaseTX, func(tx any) error {
		b := tx.(*BaseTX)
		if b.Logger() == nil {
			t.Error("Logger() returned nil")
		}
		if b.TX() == nil {
			t.Error("TX() returned nil")
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
