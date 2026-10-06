// Command example demonstrates a small LiteX-backed user store: connection setup, migrations,
// typed transactions, and CRUD built on the query helpers.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/rs/zerolog"

	"github.com/robtme/litex"
	"github.com/robtme/litex/example/domain"
	"github.com/robtme/litex/example/sqlite"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	logger := zerolog.New(os.Stdout)

	// Use litex.MemoryDSN for an ephemeral database, or a file path such as "data/app.db".
	db := sqlite.NewDB(litex.MemoryDSN, logger)
	if err := db.Open(); err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// Write: create a user.
	user := &domain.User{Email: "ada@example.com", Name: "Ada Lovelace", Role: "admin"}

	if err := db.CommitWrite(ctx, func(tx domain.WriteTX) error {
		return tx.CreateUser(ctx, user)
	}); err != nil {
		return err
	}

	// Read: fetch it back.
	return db.CommitRead(ctx, func(tx domain.ReadTX) error {
		found, err := tx.FindUserByID(ctx, user.ID)
		if err != nil {
			return err
		}

		fmt.Printf("found user %s (%s)\n", found.Name, found.Email)

		return nil
	})
}
