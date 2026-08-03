package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/wolveix/litex"

	"github.com/wolveix/litex/example/domain"
)

// Ensure the concrete transactions satisfy the domain interfaces.
var _ domain.BatchTX = (*BatchTX)(nil)
var _ domain.ReadTX = (*ReadTX)(nil)
var _ domain.WriteTX = (*WriteTX)(nil)

// CreateUser inserts a new user.
func (w *WriteTX) CreateUser(ctx context.Context, user *domain.User) error {
	user.ID = newID()
	user.Created = time.Now()
	user.Updated = user.Created

	if _, err := w.Exec(
		ctx,
		`INSERT INTO users (id, created, updated, email, name, role) VALUES (?, ?, ?, ?, ?, ?)`,
		user.ID, user.Created, user.Updated, user.Email, user.Name, user.Role,
	); err != nil {
		return fmt.Errorf("create user: %w", err)
	}

	return nil
}

// FindUsers returns every user matching filter.
func (r *ReadTX) FindUsers(ctx context.Context, filter *domain.UserFilter) ([]*domain.User, error) {
	if filter == nil {
		filter = &domain.UserFilter{}
	}

	stmt, args, err := litex.BuildQuery("users", "id", "id, created, updated, email, name, role", &filter.Filter,
		func(where []string, args []any) ([]string, []any) {
			if filter.ID != "" {
				where, args = append(where, litex.Where("id", litex.OpExact)), append(args, filter.ID)
			}

			if filter.Email != "" {
				where, args = append(where, litex.Where("email", litex.OpExact)), append(args, filter.Email)
			}

			if filter.Name != "" {
				// OpExact would be wrong here: partial-name search is the point.
				where, args = append(where, litex.Where("name", litex.OpSubstring)), append(args, filter.Name)
			}

			if filter.Role != "" {
				where, args = append(where, litex.Where("role", litex.OpExact)), append(args, filter.Role)
			}

			return where, args
		})
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	users := make([]*domain.User, 0)

	if err = r.Find(ctx, stmt, args, func(rows *sql.Rows) error {
		var user domain.User
		if err := rows.Scan(&user.ID, &user.Created, &user.Updated, &user.Email, &user.Name, &user.Role); err != nil {
			return fmt.Errorf("scan: %w", err)
		}

		users = append(users, &user)

		return nil
	}); err != nil {
		return nil, fmt.Errorf("find users: %w", err)
	}

	return users, nil
}

// FindUserByID is a convenience wrapper over FindUsers.
func (r *ReadTX) FindUserByID(ctx context.Context, id string) (*domain.User, error) {
	users, err := r.FindUsers(ctx, &domain.UserFilter{Filter: litex.Filter{Limit: 1}, ID: id})
	if err != nil {
		return nil, err
	}

	if len(users) == 0 {
		return nil, fmt.Errorf("user %q not found", id)
	}

	return users[0], nil
}

// UpdateUser applies the non-nil fields of update. UpdateQuery builds the SET list; the caller
// appends the WHERE clause and its argument.
func (w *WriteTX) UpdateUser(ctx context.Context, id string, update *domain.UserUpdate) error {
	stmt, args, err := litex.UpdateQuery("users", func(fields []string, args []any) ([]string, []any) {
		if update.Email != nil {
			fields, args = append(fields, "email"), append(args, *update.Email)
		}

		if update.Name != nil {
			fields, args = append(fields, "name"), append(args, *update.Name)
		}

		if update.Role != nil {
			fields, args = append(fields, "role"), append(args, *update.Role)
		}

		fields, args = append(fields, "updated"), append(args, time.Now())

		return fields, args
	})
	if err != nil {
		return fmt.Errorf("build update: %w", err)
	}

	args = append(args, id)

	if _, err = w.Exec(ctx, stmt+" WHERE id = ?", args...); err != nil {
		return fmt.Errorf("update user: %w", err)
	}

	return nil
}

// DeleteUser removes a user by ID.
func (w *WriteTX) DeleteUser(ctx context.Context, id string) error {
	if _, err := w.Exec(ctx, `DELETE FROM users WHERE id = ?`, id); err != nil {
		return fmt.Errorf("delete user: %w", err)
	}

	return nil
}

// QueueUser buffers a user insert for the next batch flush.
func (b *BatchTX) QueueUser(ctx context.Context, user *domain.User) error {
	user.ID = newID()
	user.Created = time.Now()
	user.Updated = user.Created

	return b.QueueExec(
		ctx,
		`INSERT INTO users (id, created, updated, email, name, role) VALUES (?, ?, ?, ?, ?, ?)`,
		user.ID, user.Created, user.Updated, user.Email, user.Name, user.Role,
	)
}
