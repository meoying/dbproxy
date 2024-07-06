//go:build e2e

package mysql

import (
	"context"
	"github.com/stretchr/testify/require"
	"time"
)

func (s *ServerTestSuite) TestTransactionCommit() {
	db, err := newDB()
	require.NoError(s.T(), err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		s.T().Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = tx.ExecContext(ctx, "insert into users(name) VALUES ('Harry')")
	if err != nil {
		s.T().Fatalf("Failed to begin transaction: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		s.T().Fatalf("Failed to commit: %v", err)
	}
}

func (s *ServerTestSuite) TestTransactionRollback() {
	db, err := newDB()
	require.NoError(s.T(), err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		s.T().Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = tx.ExecContext(ctx, "insert into users(name) VALUES ('Harry')")
	if err != nil {
		s.T().Fatalf("Failed to begin transaction: %v", err)
	}

	err = tx.Rollback()
	if err != nil {
		s.T().Fatalf("Failed to commit: %v", err)
	}
}
