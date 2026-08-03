// Package domain declares the application's models and transaction interfaces. The sqlite package
// implements these interfaces, so the rest of the application depends only on domain, never on
// LiteX directly.
package domain

import (
	"context"
	"time"

	"github.com/wolveix/litex"
)

// User is a stored user record.
type User struct {
	ID      string
	Created time.Time
	Updated time.Time
	Email   string
	Name    string
	Role    string
}

// UserFilter embeds litex.Filter for limit, offset, and ordering, and adds domain-specific filters.
type UserFilter struct {
	litex.Filter

	ID    string
	Email string
	Name  string
	Role  string
}

// UserUpdate carries only the fields a caller wants to change; a nil field is left unchanged.
type UserUpdate struct {
	Email *string
	Name  *string
	Role  *string
}

// ReadTX is a read-only transaction.
type ReadTX interface {
	FindUsers(ctx context.Context, filter *UserFilter) ([]*User, error)
	FindUserByID(ctx context.Context, id string) (*User, error)
}

// WriteTX is a read-write transaction.
type WriteTX interface {
	ReadTX

	CreateUser(ctx context.Context, user *User) error
	UpdateUser(ctx context.Context, id string, update *UserUpdate) error
	DeleteUser(ctx context.Context, id string) error
}

// BatchTX is a buffered write transaction.
type BatchTX interface {
	QueueUser(ctx context.Context, user *User) error
}
