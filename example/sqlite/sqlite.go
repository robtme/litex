// Package sqlite implements the domain transaction interfaces on top of LiteX.
package sqlite

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"

	"github.com/robtme/litex"
	"github.com/rs/zerolog"

	"github.com/robtme/litex/example/domain"
)

//go:embed migrations/*.sql
var migrations embed.FS

// ReadTX is the project-specific read-only transaction.
type ReadTX struct {
	*litex.BaseTX
}

// WriteTX is the project-specific read-write transaction.
type WriteTX struct {
	*ReadTX
}

// BatchTX is the project-specific buffered write transaction.
type BatchTX struct {
	*litex.BatchTX
}

// Service wraps *litex.Service so callers depend on this package, not LiteX directly.
type Service struct {
	*litex.Service
}

// NewDB constructs the service.
func NewDB(dsn string, logger zerolog.Logger) *Service {
	return &Service{Service: litex.NewDB(dsn, logger)}
}

// Open opens the connections and applies migrations.
func (s *Service) Open() error {
	if err := s.Service.Open(); err != nil {
		return err
	}

	return s.Service.Migrate(context.Background(), migrations)
}

// CommitRead runs fn inside a read-only transaction.
func (s *Service) CommitRead(ctx context.Context, fn func(tx domain.ReadTX) error) error {
	return s.Service.CommitRead(
		ctx,
		func(base *litex.BaseTX) any { return &ReadTX{BaseTX: base} },
		func(tx any) error { return fn(tx.(*ReadTX)) },
	)
}

// CommitWrite runs fn inside a read-write transaction.
func (s *Service) CommitWrite(ctx context.Context, fn func(tx domain.WriteTX) error) error {
	return s.Service.CommitWrite(
		ctx,
		func(base *litex.BaseTX) any { return &WriteTX{ReadTX: &ReadTX{BaseTX: base}} },
		func(tx any) error { return fn(tx.(*WriteTX)) },
	)
}

// CommitBatch runs fn inside a buffered write transaction.
func (s *Service) CommitBatch(ctx context.Context, fn func(tx domain.BatchTX) error) error {
	return s.Service.CommitBatch(
		ctx,
		func(base *litex.BatchTX) any { return &BatchTX{BatchTX: base} },
		func(tx any) error { return fn(tx.(*BatchTX)) },
	)
}

// newID returns a random 128-bit hex identifier.
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // Reading from crypto/rand should never fail.
	}

	return hex.EncodeToString(b[:])
}
