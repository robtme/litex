# LiteX

[![Go Reference](https://pkg.go.dev/badge/github.com/robtme/litex.svg)](https://pkg.go.dev/github.com/robtme/litex)
[![Test](https://github.com/robtme/litex/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/robtme/litex/actions/workflows/test.yml)
[![Lint](https://github.com/robtme/litex/actions/workflows/lint.yml/badge.svg?branch=main)](https://github.com/robtme/litex/actions/workflows/lint.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Shared SQLite plumbing for Go services: tuned connection setup, transaction helpers, embedded migrations, and small
query builders. Pure Go via [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite), no cgo.

Extracted from years of production use and made public so its improvements are easy to share. Inspired by Ben
Johnson's [WTF Dial](https://github.com/benbjohnson/wtf) and its approach to structuring Go applications around a domain
package with SQLite-backed implementations.

## Not an ORM

LiteX is not an ORM, and it has no ambition to become one. There is no struct-to-table mapping, no reflection, no
relationship graph, and no query DSL to learn. You write SQL.

What it removes is the minimal, repetitive boilerplate around SQL: opening tuned read/write connections, scoping
transactions, running migrations, and assembling the filtered `SELECT`/`UPDATE` strings that every service otherwise
rewrites by hand. The query builders are thin string helpers; you still supply your own columns, `WHERE` clauses, and
row scans.

Raw SQL stays first-class and is never discouraged. `Exec`, `Query`, `QueryRow`, and `Find` all take plain SQL and
arguments, and `BaseTX.TX()` hands you the underlying `*sql.Tx` for anything the helpers do not cover. If a query reads
better as hand-written SQL, write it as hand-written SQL.

## Features

- **Split read/write pools** - a single writer connection (SQLite allows only one) plus a pool of read-only connections,
  each with sensible pragmas (WAL, `busy_timeout`, foreign keys, `synchronous=NORMAL`, mmap).
- **`BEGIN IMMEDIATE` writes** - the write connection takes its lock up front, avoiding `SQLITE_BUSY_SNAPSHOT` errors
  that `busy_timeout` will not retry.
- **Embedded migrations** - versioned `migrations/N.sql` files applied in order against `PRAGMA user_version`, with
  foreign keys safely disabled during table rebuilds, an integrity check before commit, and a `VACUUM` afterwards when
  disk space allows.
- **Transaction helpers** - `CommitRead`, `CommitWrite`, and a buffered `CommitBatch`, each adapting a base transaction
  into your own project-specific type.
- **Query builders** - `BuildQuery`, `BuildComplexQuery` (joins and `GROUP BY`), `UpdateQuery`, and `Where`, driven by a
  `Filter` that stays out of your JSON (every field is `json:"-"`).
- **Structured logging** - every statement logs through [zerolog](https://github.com/rs/zerolog).

## Install

```sh
go get github.com/robtme/litex
```

## Quick start

The raw primitives, using an in-memory database. See [Recommended pattern](#recommended-pattern) for how real
applications wrap these.

```go
ctx := context.Background()
logger := zerolog.New(os.Stdout)

// Use litex.MemoryDSN for an in-memory database, or a file path such as "data/app.db".
db := litex.NewDB(litex.MemoryDSN, logger)
if err := db.Open(); err != nil {
	panic(err)
}
defer db.Close()

// migrations is an embed.FS (or any fs.FS) holding migrations/*.sql.
if err := db.Migrate(ctx, migrations); err != nil {
	panic(err)
}

// The newTx callback adapts the base transaction; here it passes the *litex.BaseTX straight through.
err := db.CommitWrite(ctx,
	func(b *litex.BaseTX) any { return b },
	func(tx any) error {
		_, err := tx.(*litex.BaseTX).Exec(ctx, `INSERT INTO users (id, email) VALUES (?, ?)`, "u1", "a@example.com")
		return err
	},
)
```

## Migrations

Place versioned SQL files under a `migrations/` directory and embed them. Files are named `<version>.sql`, where version
is a positive integer; an optional `_description` may follow (`1.sql`, `1_init.sql`, `20260716_add_users.sql`). `Migrate`
discovers the files that exist, orders them by version, applies any newer than the database's `PRAGMA user_version` in a
single transaction, and records the highest version applied. Versions need not be contiguous, so plain integers and later
date-based versions (`YYYYMMDD`) can coexist.

```
migrations/
  1.sql
  2.sql
```

```sql
-- migrations/1.sql
CREATE TABLE users
(
    id      TEXT PRIMARY KEY,
    created TIMESTAMP NOT NULL,
    updated TIMESTAMP NOT NULL,
    email   TEXT      NOT NULL UNIQUE,
    name    TEXT      NOT NULL DEFAULT '',
    role    TEXT      NOT NULL DEFAULT 'member'
);
```

Foreign key enforcement is disabled for the duration of the migration transaction so table-rebuild patterns (create new,
copy, drop old, rename) do not cascade-delete child rows, then re-enabled and verified with `PRAGMA foreign_key_check`
before commit.

Migrations are forward-only by design: there are no down or rollback scripts. To undo a change, add a new migration that
reverses it; in development, delete the database file and start clean. Do not edit a migration once it has been applied,
since `Migrate` keys off `PRAGMA user_version` and skips any file at or below the current version.

If the database's version is newer than the newest migration a build ships, `Migrate` returns an error rather than run
against a schema the build does not recognise, catching an older binary deployed against a database a newer one already
migrated.

## Recommended pattern

For real applications, follow the WTF Dial layout: a `domain` package declares your models and transaction interfaces,
and a `sqlite` package implements them by wrapping LiteX. Wrapping the transaction helpers once gives callers typed
transactions instead of `any`, and your CRUD methods hang off those transaction types using plain SQL and the query
helpers.

```go
// Wrap *litex.Service so callers depend on your package and get typed transactions.
type Service struct {
	*litex.Service
}

type ReadTX struct{ *litex.BaseTX }
type WriteTX struct{ *ReadTX }

func (s *Service) CommitWrite(ctx context.Context, fn func(tx *WriteTX) error) error {
	return s.Service.CommitWrite(
		ctx,
		func(base *litex.BaseTX) any { return &WriteTX{ReadTX: &ReadTX{BaseTX: base}} },
		func(tx any) error { return fn(tx.(*WriteTX)) },
	)
}

// CreateUser hangs off your write transaction and uses plain SQL.
func (w *WriteTX) CreateUser(ctx context.Context, u *User) error {
	_, err := w.Exec(ctx, `INSERT INTO users (id, email) VALUES (?, ?)`, u.ID, u.Email)
	return err
}
```

See the [`example/`](example) directory for a complete, runnable application: domain interfaces, the full sqlite
implementation, CRUD via the query builders, and batched writes with `CommitBatch`.

## Batched writes

`CommitBatch` buffers writes in memory and flushes them to a real transaction on a timer (and on `Close`), for
high-volume, latency-tolerant inserts. Queue statements with `QueueExec` instead of `Exec`. See
[`example/`](example) for a worked `QueueUser` method.

`QueueExec` only fills the transaction's own list; those statements reach the shared buffer when the `CommitBatch`
callback returns `nil`. A callback that returns an error writes nothing, as with `CommitWrite`.

Past that point the buffer is service-wide, not per-`CommitBatch`: a flush commits everything buffered in that window as
one transaction, so one failing statement rolls back and drops every entry in it, including other callers' writes.
Because `QueueExec` returns before the write runs, it cannot report that failure; register `OnBatchFlushError` to
observe async flush errors, and check the error returned by `Close` (which flushes any remainder on shutdown). Use
`CommitWrite` for writes whose failure must be attributable to their own caller.

A flush that cannot open its transaction at all (the single write connection busy with a migration, `SQLITE_BUSY` past
`busy_timeout`) puts its entries back for the next flush, since nothing was attempted. A failure after a statement has
run drops the batch, since SQLite will reject that statement again.

## Filtering

`litex.Filter` supplies limit, ordering, keyset pagination, and grouping to the query builders.

```go
type Filter struct {
	Offset              any    // Keyset cursor: excludes rows ordered past this value on the OrderBy column.
	GroupBy             string // Comma-separated columns to GROUP BY.
	OrderBy             string // Column to order by; defaults to the primary field.
	Limit               int    // Max rows to return; <= 0 omits the LIMIT clause (all rows).
	ExactMatchByDefault bool   // Make operator-less clauses match the whole value; defaults to substring.
	OrderAsc            bool   // Order ascending; defaults to descending.
}
```

The zero value returns all rows ordered by the primary field descending. LiteX never caps or truncates silently, so if
your API needs a maximum, clamp `Limit` before building the query.

`Offset` is keyset pagination, not SQL `OFFSET`: it seeks on the column the rows are ordered by, becoming
`WHERE <OrderBy> < ?` (or `>` when `OrderAsc` is set), so pass the last row's value for that column, not its ID. With
`OrderBy` unset that column is the primary field. For stable paging the ordering column should be unique, otherwise ties
at a page boundary may be skipped.

### Where operators

`Where(field, operator...)` builds a `field <op> ?` fragment. Three forms are special-cased by the builder:

| You write | Emitted SQL | Meaning |
| --- | --- | --- |
| `Where("name")` | `name LIKE ?` with `%value%`, or `name = ?` | follows `Filter.ExactMatchByDefault` (substring unless set) |
| `Where("name", litex.OpSubstring)` | `name LIKE ?` with `%value%` | always substring |
| `Where("id", litex.OpExact)` | `id = ?` | always whole-value equality |
| `Where("age", ">")` | `age > ?` | passed through as-is |

Substring matching silently over-matches numeric columns, since `12` matches `1`, `2` and `123`, so keep it to free
text. A rewrite applies only where the operator is directly followed by a placeholder, so multi-value clauses (`IN`,
`BETWEEN`) and comparisons against literals (`deleted = 0`) pass through untouched. A single clause may carry several
rewritable operators, and each wraps the value bound to it.

If most clauses in a query are exact, set `Filter.ExactMatchByDefault` instead of repeating `OpExact`:

```go
// role and status match whole values; only name is a substring search.
filter := litex.Filter{ExactMatchByDefault: true}

where = append(where, litex.Where("role"), litex.Where("status"))
where = append(where, litex.Where("name", litex.OpSubstring))
```

The setting only decides what a plain `=` means, so `OpSubstring` and `OpExact` pin individual clauses either way.

Both operators are only rewritten by the query builders. A clause carrying one that you execute directly, without
passing it through `BuildQuery` or `BuildComplexQuery`, runs as-is and neither errors: SQLite reads `==` as a synonym
for `=`, and parses `name =~ ?` as `name = ~?`, a comparison against the bitwise complement of the bound value, which
silently matches nothing. Build clauses with `Where` and pass them to a builder.

Substring values are matched literally. Any `%` or `_` in the bound value is escaped, so a search term typed by a user
cannot act as a wildcard.

Identifiers you pass in are interpolated, not parameterized. `primaryField` and the `Filter` fields `OrderBy` and
`GroupBy` are validated as simple or dotted column identifiers (`id`, `users.id`), so expressions and quoted identifiers
such as `COALESCE(updated, created)` or `"order"` are rejected at query-build time. `table`, `fields`, `joins` and
`BuildComplexQuery`'s `groupBy` argument are unvalidated: they may hold expressions, but must be trusted,
developer-controlled values, never request data.

## License

[MIT](LICENSE).

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=robtme/litex&type=Date)](https://star-history.com/#robtme/litex&Date)
