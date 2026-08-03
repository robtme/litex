package litex

import (
	"context"
	"embed"
	"io/fs"
	"testing"
	"testing/fstest"
)

//go:embed migrations/*.sql
var testMigrations embed.FS

func TestMigrateApply(t *testing.T) {
	tests := []struct {
		name        string
		migrations  fs.FS
		wantVersion int
		wantTables  []string
	}{
		{
			name:        "embedded set applies every file",
			migrations:  testMigrations,
			wantVersion: 2,
			wantTables:  []string{"mig_test", "parent", "child"},
		},
		{
			name: "non-contiguous versions",
			migrations: fstest.MapFS{
				"migrations/1.sql":        {Data: []byte("CREATE TABLE a (id INTEGER PRIMARY KEY);")},
				"migrations/20260716.sql": {Data: []byte("CREATE TABLE b (id INTEGER PRIMARY KEY);")},
			},
			wantVersion: 20260716,
			wantTables:  []string{"a", "b"},
		},
		{
			name: "description suffix",
			migrations: fstest.MapFS{
				"migrations/1_init.sql": {Data: []byte("CREATE TABLE c (id INTEGER PRIMARY KEY);")},
			},
			wantVersion: 1,
			wantTables:  []string{"c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openFileDB(t)

			if err := s.Migrate(context.Background(), tt.migrations); err != nil {
				t.Fatalf("Migrate: %v", err)
			}

			var version int
			if err := s.connRO.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
				t.Fatal(err)
			}
			if version != tt.wantVersion {
				t.Errorf("user_version = %d, want %d", version, tt.wantVersion)
			}

			for _, table := range tt.wantTables {
				var name string
				if err := s.connRO.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&name); err != nil {
					t.Errorf("table %q missing: %v", table, err)
				}
			}
		})
	}
}

func TestMigrateNoOp(t *testing.T) {
	s := openFileDB(t)
	ctx := context.Background()

	if err := s.Migrate(ctx, testMigrations); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if err := s.Migrate(ctx, testMigrations); err != nil {
		t.Fatalf("second migrate (should be no-op): %v", err)
	}
}

func TestMigrateEmpty(t *testing.T) {
	// Far more often a mistyped embed than a service with genuinely no schema, so it must not pass
	// silently.
	tests := map[string]fs.FS{
		"no files":                     fstest.MapFS{},
		"wrong directory":              fstest.MapFS{"sql/1.sql": {Data: []byte("SELECT 1;")}},
		"rooted at the migrations dir": fstest.MapFS{"1.sql": {Data: []byte("SELECT 1;")}},
	}

	for name, fsys := range tests {
		t.Run(name, func(t *testing.T) {
			s := openFileDB(t)

			if err := s.Migrate(context.Background(), fsys); err == nil {
				t.Error("expected an error when no migrations are found")
			}

			var version int
			if err := s.connRO.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
				t.Fatal(err)
			}
			if version != 0 {
				t.Errorf("user_version = %d, want 0", version)
			}
		})
	}
}

// TestMigrateForeignKeyRebuild verifies that a table-rebuild migration preserves child rows (which
// FK enforcement would otherwise cascade-delete) and restores enforcement afterwards.
func TestMigrateForeignKeyRebuild(t *testing.T) {
	s := openFileDB(t)

	if err := s.Migrate(context.Background(), testMigrations); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	var childCount int
	if err := s.connRO.QueryRow("SELECT COUNT(*) FROM child").Scan(&childCount); err != nil {
		t.Fatal(err)
	}
	if childCount != 1 {
		t.Errorf("child rows = %d, want 1 (survived rebuild)", childCount)
	}

	var fk int
	if err := s.connRW.QueryRow("PRAGMA foreign_keys").Scan(&fk); err != nil {
		t.Fatal(err)
	}
	if fk != 1 {
		t.Errorf("foreign_keys = %d, want 1 (restored after migration)", fk)
	}
}

func TestMigrateErrors(t *testing.T) {
	tests := []struct {
		name       string
		migrations fs.FS
		open       bool
		setup      func(t *testing.T, s *Service)
	}{
		{
			name:       "database not open",
			migrations: testMigrations,
			open:       false,
		},
		{
			name:       "non-numeric filename",
			migrations: fstest.MapFS{"migrations/init.sql": {Data: []byte("SELECT 1;")}},
			open:       true,
		},
		{
			name: "duplicate version",
			migrations: fstest.MapFS{
				"migrations/1.sql":     {Data: []byte("CREATE TABLE a (id INTEGER PRIMARY KEY);")},
				"migrations/1_dup.sql": {Data: []byte("CREATE TABLE b (id INTEGER PRIMARY KEY);")},
			},
			open: true,
		},
		{
			name: "foreign key violation",
			migrations: fstest.MapFS{
				"migrations/1.sql": {Data: []byte(`
					CREATE TABLE parent (id INTEGER PRIMARY KEY);
					CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES parent (id));
					INSERT INTO child (id, parent_id) VALUES (1, 999);
				`)},
			},
			open: true,
		},
		{
			name:       "database newer than build",
			migrations: fstest.MapFS{"migrations/1.sql": {Data: []byte("CREATE TABLE a (id INTEGER PRIMARY KEY);")}},
			open:       true,
			setup: func(t *testing.T, s *Service) {
				ahead := fstest.MapFS{"migrations/5.sql": {Data: []byte("CREATE TABLE z (id INTEGER PRIMARY KEY);")}}
				if err := s.Migrate(context.Background(), ahead); err != nil {
					t.Fatalf("setup migrate: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s *Service
			if tt.open {
				s = openFileDB(t)
			} else {
				s = NewDB(MemoryDSN, testLogger()) // Deliberately not opened.
			}

			if tt.setup != nil {
				tt.setup(t, s)
			}

			if err := s.Migrate(context.Background(), tt.migrations); err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestParseMigrationVersion(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		want    int
		wantErr bool
	}{
		{name: "plain integer", file: "migrations/1.sql", want: 1},
		{name: "date based", file: "migrations/20260716.sql", want: 20260716},
		{name: "description suffix", file: "migrations/1_init.sql", want: 1},
		{name: "date with description", file: "migrations/20260716_add_users.sql", want: 20260716},
		{name: "base name only", file: "5.sql", want: 5},
		{name: "non-numeric", file: "migrations/init.sql", wantErr: true},
		{name: "trailing letters", file: "migrations/1a.sql", wantErr: true},
		{name: "zero", file: "migrations/0.sql", wantErr: true},
		{name: "negative", file: "migrations/-1.sql", wantErr: true},
		{name: "overflows user_version", file: "migrations/99999999999.sql", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMigrationVersion(tt.file)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for %q", tt.file)
				}

				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("version = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestLoadMigrations(t *testing.T) {
	tests := []struct {
		name    string
		fsys    fs.FS
		want    []int
		wantErr bool
	}{
		{
			name: "sorted ascending",
			fsys: fstest.MapFS{
				"migrations/2.sql":  {Data: []byte("SELECT 1;")},
				"migrations/1.sql":  {Data: []byte("SELECT 1;")},
				"migrations/10.sql": {Data: []byte("SELECT 1;")},
			},
			want: []int{1, 2, 10},
		},
		{
			name: "duplicate version",
			fsys: fstest.MapFS{
				"migrations/1.sql":     {Data: []byte("SELECT 1;")},
				"migrations/1_dup.sql": {Data: []byte("SELECT 1;")},
			},
			wantErr: true,
		},
		{
			name: "empty",
			fsys: fstest.MapFS{},
			want: []int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loadMigrations(tt.fsys)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}

				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			versions := make([]int, 0, len(got))
			for _, m := range got {
				versions = append(versions, m.version)
			}

			if len(versions) != len(tt.want) {
				t.Fatalf("versions = %v, want %v", versions, tt.want)
			}
			for i := range versions {
				if versions[i] != tt.want[i] {
					t.Fatalf("versions = %v, want %v", versions, tt.want)
				}
			}
		})
	}
}
