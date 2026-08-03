package litex

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
)

func TestBuildQuery(t *testing.T) {
	tests := []struct {
		name      string
		filter    *Filter
		build     func([]string, []any) ([]string, []any)
		wantQuery string
		wantArgs  []any
	}{
		{
			name:   "filter with limit offset and ordering",
			filter: &Filter{Limit: 10, Offset: 5, OrderAsc: true, OrderBy: "name"},
			build: func(where []string, args []any) ([]string, []any) {
				return append(where, Where("email", OpExact)), append(args, "test@example.com")
			},
			wantQuery: "SELECT * FROM users WHERE email = ? AND name > ? ORDER BY name ASC LIMIT 10",
			wantArgs:  []any{"test@example.com", 5},
		},
		{
			name:   "default operator becomes LIKE",
			filter: nil,
			build: func(where []string, args []any) ([]string, []any) {
				return append(where, Where("name")), append(args, "John")
			},
			wantQuery: "SELECT * FROM users WHERE name LIKE ? ESCAPE '\\' ORDER BY id DESC",
			wantArgs:  []any{"%John%"},
		},
		{
			name:      "nil filter omits limit",
			filter:    nil,
			build:     func(where []string, args []any) ([]string, []any) { return where, args },
			wantQuery: "SELECT * FROM users WHERE 1 = 1 ORDER BY id DESC",
			wantArgs:  nil,
		},
		{
			name:      "zero limit on a non-nil filter still omits limit",
			filter:    &Filter{OrderBy: "created_at", OrderAsc: true},
			build:     func(where []string, args []any) ([]string, []any) { return where, args },
			wantQuery: "SELECT * FROM users WHERE 1 = 1 ORDER BY created_at ASC",
			wantArgs:  nil,
		},
		{
			name:   "more where clauses than args does not panic",
			filter: nil,
			build: func(where []string, args []any) ([]string, []any) {
				return append(where, "name == ?"), args
			},
			wantQuery: "SELECT * FROM users WHERE name = ? ORDER BY id DESC",
			wantArgs:  nil,
		},
		{
			name:   "exact match by default leaves the value unwrapped",
			filter: &Filter{ExactMatchByDefault: true},
			build: func(where []string, args []any) ([]string, []any) {
				return append(where, Where("name")), append(args, "John")
			},
			wantQuery: "SELECT * FROM users WHERE name = ? ORDER BY id DESC",
			wantArgs:  []any{"John"},
		},
		{
			name:   "exact match by default still normalizes OpExact clauses",
			filter: &Filter{ExactMatchByDefault: true},
			build: func(where []string, args []any) ([]string, []any) {
				return append(where, Where("role", OpExact)), append(args, "admin")
			},
			wantQuery: "SELECT * FROM users WHERE role = ? ORDER BY id DESC",
			wantArgs:  []any{"admin"},
		},
		{
			name:   "exact match by default leaves a hand-written LIKE alone",
			filter: &Filter{ExactMatchByDefault: true},
			build: func(where []string, args []any) ([]string, []any) {
				return append(where, "name LIKE ?"), append(args, "%John%")
			},
			wantQuery: "SELECT * FROM users WHERE name LIKE ? ORDER BY id DESC",
			wantArgs:  []any{"%John%"},
		},
		{
			// The point of OpSubstring being its own token: one field opts back into substring matching
			// while the rest of the query stays exact.
			name:   "explicit OpSubstring overrides an exact default",
			filter: &Filter{ExactMatchByDefault: true},
			build: func(where []string, args []any) ([]string, []any) {
				where = append(where, Where("name", OpSubstring), Where("role"))
				return where, append(args, "John", "admin")
			},
			wantQuery: "SELECT * FROM users WHERE name LIKE ? ESCAPE '\\' AND role = ? ORDER BY id DESC",
			wantArgs:  []any{"%John%", "admin"},
		},
		{
			// The mirror case: one field pinned exact while the query default stays substring.
			name:   "explicit OpExact overrides a substring default",
			filter: nil,
			build: func(where []string, args []any) ([]string, []any) {
				where = append(where, Where("name"), Where("role", OpExact))
				return where, append(args, "John", "admin")
			},
			wantQuery: "SELECT * FROM users WHERE name LIKE ? ESCAPE '\\' AND role = ? ORDER BY id DESC",
			wantArgs:  []any{"%John%", "admin"},
		},
		{
			// The wildcard must land on the value bound to =~, not on the clause's first placeholder.
			name:   "OpSubstring wraps its own arg in a multi-placeholder clause",
			filter: nil,
			build: func(where []string, args []any) ([]string, []any) {
				return append(where, "age > ? AND name "+OpSubstring+" ?"), append(args, 18, "John")
			},
			wantQuery: "SELECT * FROM users WHERE age > ? AND name LIKE ? ESCAPE '\\' ORDER BY id DESC",
			wantArgs:  []any{18, "%John%"},
		},
		{
			name:   "multi-placeholder clause keeps a later LIKE arg aligned",
			filter: nil,
			build: func(where []string, args []any) ([]string, []any) {
				where = append(where, "age BETWEEN ? AND ?")
				args = append(args, 18, 30)
				where = append(where, Where("name"))
				args = append(args, "John")

				return where, args
			},
			wantQuery: "SELECT * FROM users WHERE age BETWEEN ? AND ? AND name LIKE ? ESCAPE '\\' ORDER BY id DESC",
			wantArgs:  []any{18, 30, "%John%"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args, err := BuildQuery("users", "id", "*", tt.filter, tt.build)
			if err != nil {
				t.Fatalf("BuildQuery: %v", err)
			}
			if query != tt.wantQuery {
				t.Errorf("query = %q, want %q", query, tt.wantQuery)
			}
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("args = %#v, want %#v", args, tt.wantArgs)
			}
		})
	}
}

func TestBuildComplexQuery(t *testing.T) {
	tests := []struct {
		name      string
		table     string
		primary   string
		fields    string
		filter    *Filter
		joins     []string
		groupBy   []string
		build     func([]string, []any) ([]string, []any)
		wantQuery string
		wantArgs  []any
		wantErr   bool
	}{
		{
			name:    "joins and group by",
			table:   "users",
			primary: "users.id",
			fields:  "users.*, profiles.bio",
			filter:  &Filter{Limit: 20},
			joins:   []string{"LEFT JOIN profiles ON users.id = profiles.user_id"},
			groupBy: []string{"users.id"},
			build: func(where []string, args []any) ([]string, []any) {
				return append(where, "users.active == ?"), append(args, 1)
			},
			wantQuery: "SELECT users.*, profiles.bio FROM users LEFT JOIN profiles ON users.id = profiles.user_id WHERE users.active = ? GROUP BY users.id ORDER BY users.id DESC LIMIT 20",
			wantArgs:  []any{1},
		},
		{
			name:      "group by wraps non-grouped fields in max()",
			table:     "t",
			primary:   "t.id",
			fields:    "t.id, t.name",
			filter:    &Filter{GroupBy: "t.id"},
			build:     func(where []string, args []any) ([]string, []any) { return where, args },
			wantQuery: "SELECT t.id, max(t.name) FROM t WHERE 1 = 1 GROUP BY t.id ORDER BY t.id DESC",
			wantArgs:  nil,
		},
		{
			name:      "group by leaves function fields intact",
			table:     "t",
			primary:   "t.id",
			fields:    "t.id, GROUP_CONCAT(tag, ',') AS tags",
			filter:    &Filter{GroupBy: "t.id"},
			build:     func(where []string, args []any) ([]string, []any) { return where, args },
			wantQuery: "SELECT t.id, GROUP_CONCAT(tag, ',') AS tags FROM t WHERE 1 = 1 GROUP BY t.id ORDER BY t.id DESC",
			wantArgs:  nil,
		},
		{
			name:      "group by leaves an aliased grouped column intact",
			table:     "t",
			primary:   "t.id",
			fields:    "t.id AS ident, t.name",
			filter:    &Filter{GroupBy: "t.id"},
			build:     func(where []string, args []any) ([]string, []any) { return where, args },
			wantQuery: "SELECT t.id AS ident, max(t.name) FROM t WHERE 1 = 1 GROUP BY t.id ORDER BY t.id DESC",
			wantArgs:  nil,
		},
		{
			// Without wrapping, SQLite returns an arbitrary row's name here rather than erroring.
			name:      "group by wraps an aliased non-grouped column",
			table:     "t",
			primary:   "t.id",
			fields:    "t.id, t.name AS n",
			filter:    &Filter{GroupBy: "t.id"},
			build:     func(where []string, args []any) ([]string, []any) { return where, args },
			wantQuery: "SELECT t.id, max(t.name) AS n FROM t WHERE 1 = 1 GROUP BY t.id ORDER BY t.id DESC",
			wantArgs:  nil,
		},
		{
			name:    "group by with star fields errors",
			table:   "t",
			primary: "t.id",
			fields:  "*",
			filter:  &Filter{GroupBy: "t.id"},
			build:   func(where []string, args []any) ([]string, []any) { return where, args },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args, err := BuildComplexQuery(tt.table, tt.primary, tt.fields, tt.filter, tt.joins, tt.groupBy, tt.build)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}

				return
			}
			if err != nil {
				t.Fatalf("BuildComplexQuery: %v", err)
			}
			if query != tt.wantQuery {
				t.Errorf("query = %q, want %q", query, tt.wantQuery)
			}
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("args = %#v, want %#v", args, tt.wantArgs)
			}
		})
	}
}

func TestUpdateQuery(t *testing.T) {
	tests := []struct {
		name      string
		build     func([]string, []any) ([]string, []any)
		wantQuery string
		wantErr   bool
	}{
		{
			name: "valid update",
			build: func(fields []string, args []any) ([]string, []any) {
				return append(fields, "name", "email"), append(args, "Jane", "jane@example.com")
			},
			wantQuery: "UPDATE users SET name = ?, email = ?",
		},
		{
			name:    "missing fields",
			build:   func(fields []string, args []any) ([]string, []any) { return fields, args },
			wantErr: true,
		},
		{
			name: "unequal fields and args",
			build: func(fields []string, args []any) ([]string, []any) {
				return append(fields, "name"), args
			},
			wantErr: true,
		},
		{
			// A handler mapping request keys to columns must not be able to smuggle SQL through one.
			name: "rejects an unsafe field name",
			build: func(fields []string, args []any) ([]string, []any) {
				return append(fields, "name = 'x' WHERE 1=1; --"), append(args, "y")
			},
			wantErr: true,
		},
		{
			name: "allows a dotted field name",
			build: func(fields []string, args []any) ([]string, []any) {
				return append(fields, "users.name"), append(args, "y")
			},
			wantQuery: "UPDATE users SET users.name = ?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, _, err := UpdateQuery("users", tt.build)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}

				return
			}
			if err != nil {
				t.Fatalf("UpdateQuery: %v", err)
			}
			if query != tt.wantQuery {
				t.Errorf("query = %q, want %q", query, tt.wantQuery)
			}
		})
	}
}

func TestOperatorsArePerClause(t *testing.T) {
	// One query mixing both special operators: the substring clause is wildcard-wrapped, the exact
	// clause is normalized to "=" and its value left alone.
	query, args, err := BuildQuery("users", "id", "*", nil, func(where []string, args []any) ([]string, []any) {
		where = append(where, Where("name", OpSubstring), Where("role", OpExact))
		return where, append(args, "John", "admin")
	})
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}

	want := "SELECT * FROM users WHERE name LIKE ? ESCAPE '\\' AND role = ? ORDER BY id DESC"
	if query != want {
		t.Errorf("query = %q, want %q", query, want)
	}
	if len(args) != 2 || args[0] != "%John%" || args[1] != "admin" {
		t.Errorf("args = %#v, want [%%John%% admin]", args)
	}
}

func TestBuildQueryRejectsUnsafeIdentifiers(t *testing.T) {
	noWhere := func(where []string, args []any) ([]string, []any) { return where, args }

	tests := []struct {
		name    string
		primary string
		filter  *Filter
	}{
		{name: "order by injection", primary: "id", filter: &Filter{OrderBy: "name; DROP TABLE users"}},
		{name: "group by injection", primary: "id", filter: &Filter{GroupBy: "1); DROP TABLE users--"}},
		{name: "primary field injection", primary: "id; DROP TABLE users"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := BuildQuery("users", tt.primary, "id, name", tt.filter, noWhere); err == nil {
				t.Error("expected error for unsafe identifier")
			}
		})
	}
}

// TestBuildQueryExecutes runs the builder's output against a real SQLite database, verifying the
// generated SQL is valid and returns the correct rows for the combinations real callers hit.
func TestBuildQueryExecutes(t *testing.T) {
	s := openMemoryDB(t)
	ctx := context.Background()

	mustExec(t, s, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	mustExec(t, s, "INSERT INTO users (id, name, age) VALUES (1,'Alice',30),(2,'Bob',25),(3,'Carol',30),(4,'Dave',40)")

	// runIDs executes stmt and collects the scanned id column.
	runIDs := func(t *testing.T, stmt string, args []any) []int {
		t.Helper()

		var ids []int
		if err := s.CommitRead(ctx, passBaseTX, func(tx any) error {
			return tx.(*BaseTX).Find(ctx, stmt, args, func(rows *sql.Rows) error {
				var id int
				if err := rows.Scan(&id); err != nil {
					return err
				}
				ids = append(ids, id)

				return nil
			})
		}); err != nil {
			t.Fatalf("execute %q: %v", stmt, err)
		}

		return ids
	}

	t.Run("strict equality with ==", func(t *testing.T) {
		stmt, args, err := BuildQuery("users", "id", "id", &Filter{OrderAsc: true}, func(w []string, a []any) ([]string, []any) {
			return append(w, Where("age", OpExact)), append(a, 30)
		})
		if err != nil {
			t.Fatal(err)
		}
		if ids := runIDs(t, stmt, args); !reflect.DeepEqual(ids, []int{1, 3}) {
			t.Errorf("ids = %v, want [1 3]", ids)
		}
	})

	t.Run("substring match treats LIKE metacharacters literally", func(t *testing.T) {
		// Unescaped, a "%" typed into a search box matched every row.
		mustExec(t, s, "INSERT INTO users (id, name, age) VALUES (5,'100%',50),(6,'a_c',60),(7,'abc',70)")

		for _, tt := range []struct {
			term string
			want []int
		}{
			{term: "%", want: []int{5}},    // Not every row.
			{term: "_", want: []int{6}},    // Not any single character.
			{term: "a_c", want: []int{6}},  // Literal, so "abc" does not match.
			{term: "100%", want: []int{5}}, // Trailing "%" is part of the term.
			{term: `\`, want: nil},         // The escape character itself is escaped.
		} {
			t.Run(tt.term, func(t *testing.T) {
				stmt, args, err := BuildQuery("users", "id", "id", &Filter{OrderAsc: true}, func(w []string, a []any) ([]string, []any) {
					return append(w, Where("name", OpSubstring)), append(a, tt.term)
				})
				if err != nil {
					t.Fatal(err)
				}
				if ids := runIDs(t, stmt, args); !reflect.DeepEqual(ids, tt.want) {
					t.Errorf("search %q matched ids %v, want %v", tt.term, ids, tt.want)
				}
			})
		}

		mustExec(t, s, "DELETE FROM users WHERE id > 4")
	})

	t.Run("substring LIKE default", func(t *testing.T) {
		stmt, args, err := BuildQuery("users", "id", "id", &Filter{OrderAsc: true}, func(w []string, a []any) ([]string, []any) {
			return append(w, Where("name")), append(a, "o") // Matches Bob and Carol.
		})
		if err != nil {
			t.Fatal(err)
		}
		if ids := runIDs(t, stmt, args); !reflect.DeepEqual(ids, []int{2, 3}) {
			t.Errorf("ids = %v, want [2 3]", ids)
		}
	})

	t.Run("IN clause with multiple placeholders", func(t *testing.T) {
		stmt, args, err := BuildQuery("users", "id", "id", &Filter{OrderAsc: true}, func(w []string, a []any) ([]string, []any) {
			return append(w, "id IN (?, ?)"), append(a, 2, 4)
		})
		if err != nil {
			t.Fatal(err)
		}
		if ids := runIDs(t, stmt, args); !reflect.DeepEqual(ids, []int{2, 4}) {
			t.Errorf("ids = %v, want [2 4]", ids)
		}
	})

	t.Run("keyset pagination by primary key", func(t *testing.T) {
		p1, a1, err := BuildQuery("users", "id", "id", &Filter{Limit: 2, OrderAsc: true}, noWhereFn)
		if err != nil {
			t.Fatal(err)
		}
		if ids := runIDs(t, p1, a1); !reflect.DeepEqual(ids, []int{1, 2}) {
			t.Fatalf("page 1 ids = %v, want [1 2]", ids)
		}

		p2, a2, err := BuildQuery("users", "id", "id", &Filter{Limit: 2, OrderAsc: true, Offset: 2}, noWhereFn)
		if err != nil {
			t.Fatal(err)
		}
		if ids := runIDs(t, p2, a2); !reflect.DeepEqual(ids, []int{3, 4}) {
			t.Errorf("page 2 ids = %v, want [3 4]", ids)
		}
	})

	t.Run("keyset pagination by custom order column", func(t *testing.T) {
		// Ordering by name, the cursor must seek on name, not the primary key.
		p2, a2, err := BuildQuery("users", "id", "id", &Filter{Limit: 2, OrderAsc: true, OrderBy: "name", Offset: "Bob"}, noWhereFn)
		if err != nil {
			t.Fatal(err)
		}
		if ids := runIDs(t, p2, a2); !reflect.DeepEqual(ids, []int{3, 4}) {
			t.Errorf("ids = %v, want [3 4] (Carol, Dave)", ids)
		}
	})

	t.Run("group by aggregates", func(t *testing.T) {
		stmt, _, err := BuildComplexQuery("users", "id", "age, COUNT(*) AS c", &Filter{GroupBy: "age", OrderBy: "age", OrderAsc: true}, nil, nil, noWhereFn)
		if err != nil {
			t.Fatal(err)
		}

		type bucket struct{ age, count int }
		var got []bucket
		if err := s.CommitRead(ctx, passBaseTX, func(tx any) error {
			return tx.(*BaseTX).Find(ctx, stmt, nil, func(rows *sql.Rows) error {
				var b bucket
				if err := rows.Scan(&b.age, &b.count); err != nil {
					return err
				}
				got = append(got, b)

				return nil
			})
		}); err != nil {
			t.Fatal(err)
		}

		want := []bucket{{25, 1}, {30, 2}, {40, 1}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("buckets = %v, want %v", got, want)
		}
	})
}

func noWhereFn(where []string, args []any) ([]string, []any) { return where, args }

func TestIsIdentifier(t *testing.T) {
	tests := []struct {
		value string
		want  bool
	}{
		{value: "id", want: true},
		{value: "users.id", want: true},
		{value: "_private", want: true},
		{value: "a.b.c", want: true},
		{value: "col2", want: true},
		{value: "", want: false},
		{value: "2col", want: false},      // A segment may not start with a digit.
		{value: ".id", want: false},       // Empty leading segment.
		{value: "id.", want: false},       // Empty trailing segment.
		{value: "a..b", want: false},      // Empty middle segment.
		{value: "users.2id", want: false}, // Digit-led segment after a dot.
		{value: "id; DROP", want: false},  // The injection case the check exists for.
		{value: "COALESCE(a,b)", want: false},
		{value: `"order"`, want: false},
		{value: "id COLLATE NOCASE", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			if got := isIdentifier(tt.value); got != tt.want {
				t.Errorf("isIdentifier(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

func TestRewriteClause(t *testing.T) {
	tests := []struct {
		name           string
		clause         string
		args           []any
		exactByDefault bool
		wantClause     string
		wantArgs       []any
		wantNextArg    int
	}{
		{
			name: "plain equality becomes LIKE", clause: "id = ?", args: []any{"a"},
			wantClause: "id LIKE ? ESCAPE '\\'", wantArgs: []any{"%a%"}, wantNextArg: 1,
		},
		{
			name: "multi-value clause is left alone", clause: "age BETWEEN ? AND ?", args: []any{1, 2},
			wantClause: "age BETWEEN ? AND ?", wantArgs: []any{1, 2}, wantNextArg: 2,
		},
		{
			// A '?' inside a string literal is data, not a placeholder, so it must not shift the count.
			name: "question mark inside a literal", clause: "note = ? AND label LIKE '%?%'", args: []any{"n"},
			wantClause: "note LIKE ? ESCAPE '\\' AND label LIKE '%?%'", wantArgs: []any{"%n%"}, wantNextArg: 1,
		},
		{
			// An "=" bound to a literal is not a comparison against an argument; rewriting it would
			// mangle the literal and wrap an argument belonging to the other half of the clause.
			name: "equality against a literal is not rewritten", clause: "deleted = 0 AND name = ?", args: []any{"a"},
			wantClause: "deleted = 0 AND name LIKE ? ESCAPE '\\'", wantArgs: []any{"%a%"}, wantNextArg: 1,
		},
		{
			name: "every OpSubstring in a clause is rewritten", clause: "name =~ ? OR email =~ ?", args: []any{"a", "b"},
			wantClause: "name LIKE ? ESCAPE '\\' OR email LIKE ? ESCAPE '\\'", wantArgs: []any{"%a%", "%b%"}, wantNextArg: 2,
		},
		{
			name: "operators mixed in one clause", clause: "role == ? AND name =~ ?", args: []any{"admin", "jo"},
			wantClause: "role = ? AND name LIKE ? ESCAPE '\\'", wantArgs: []any{"admin", "%jo%"}, wantNextArg: 2,
		},
		{
			name: "OpSubstring ignores an exact default", clause: "name =~ ?", args: []any{"a"}, exactByDefault: true,
			wantClause: "name LIKE ? ESCAPE '\\'", wantArgs: []any{"%a%"}, wantNextArg: 1,
		},
		{
			name: "plain equality follows an exact default", clause: "name = ?", args: []any{"a"}, exactByDefault: true,
			wantClause: "name = ?", wantArgs: []any{"a"}, wantNextArg: 1,
		},
		{
			// The surplus placeholder has no value to wrap, so it is skipped rather than indexed.
			name: "more placeholders than bound args", clause: "name =~ ? OR email =~ ?", args: []any{"a"},
			wantClause: "name LIKE ? ESCAPE '\\' OR email LIKE ? ESCAPE '\\'", wantArgs: []any{"%a%"}, wantNextArg: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, next, err := rewriteClause(tt.clause, tt.args, 0, tt.exactByDefault)
			if err != nil {
				t.Fatalf("rewriteClause: %v", err)
			}
			if got != tt.wantClause {
				t.Errorf("clause = %q, want %q", got, tt.wantClause)
			}
			if next != tt.wantNextArg {
				t.Errorf("next arg index = %d, want %d", next, tt.wantNextArg)
			}
			if !reflect.DeepEqual(tt.args, tt.wantArgs) {
				t.Errorf("args = %#v, want %#v", tt.args, tt.wantArgs)
			}
		})
	}
}

func TestSubstringMatchRejectsUnstringableValue(t *testing.T) {
	// These have no string form, so cast would yield "", building LIKE '%%': a filter meant to narrow
	// the result set would return the whole table instead.
	values := []any{
		[16]byte{1, 2, 3}, // Stands in for uuid.UUID.
		struct{ A int }{1},
		nil,
	}

	// Both routes into the substring rewrite must reject the value, not just the default one.
	clauses := []struct {
		name  string
		where string
	}{
		{name: "default", where: Where("id")},
		{name: "explicit OpSubstring", where: Where("id", OpSubstring)},
	}

	for _, v := range values {
		for _, c := range clauses {
			t.Run(fmt.Sprintf("%T/%s", v, c.name), func(t *testing.T) {
				_, _, err := BuildQuery("users", "id", "*", nil, func(where []string, args []any) ([]string, []any) {
					return append(where, c.where), append(args, v)
				})
				if err == nil {
					t.Errorf("expected an error for a %T bound to a substring match", v)
				}
			})
		}
	}

	// A value with a string form is still wrapped as usual.
	_, args, err := BuildQuery("users", "id", "*", nil, func(where []string, args []any) ([]string, []any) {
		return append(where, Where("id")), append(args, 42)
	})
	if err != nil {
		t.Fatalf("BuildQuery: %v", err)
	}
	if len(args) != 1 || args[0] != "%42%" {
		t.Errorf("args = %#v, want [%%42%%]", args)
	}
}

func TestSplitTopLevel(t *testing.T) {
	tests := []struct {
		name   string
		fields string
		want   []string
	}{
		{name: "plain", fields: "id, name", want: []string{"id", " name"}},
		{name: "function args", fields: "id, GROUP_CONCAT(a, b)", want: []string{"id", " GROUP_CONCAT(a, b)"}},
		{name: "string literal", fields: "'a,b' AS lit, id", want: []string{"'a,b' AS lit", " id"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitTopLevel(tt.fields)
			if len(got) != len(tt.want) {
				t.Fatalf("splitTopLevel(%q) = %q, want %q", tt.fields, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("splitTopLevel(%q)[%d] = %q, want %q", tt.fields, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestWhere(t *testing.T) {
	tests := []struct {
		name     string
		field    string
		operator []string
		want     string
	}{
		// The want values are literals, so these pin the exact strings buildQueryCore scans for.
		{name: "default operator", field: "id", want: "id = ?"},
		{name: "substring", field: "id", operator: []string{OpSubstring}, want: "id =~ ?"},
		{name: "exact", field: "id", operator: []string{OpExact}, want: "id == ?"},
		{name: "comparison", field: "id", operator: []string{">"}, want: "id > ?"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Where(tt.field, tt.operator...); got != tt.want {
				t.Errorf("Where(%q, %v) = %q, want %q", tt.field, tt.operator, got, tt.want)
			}
		})
	}
}
