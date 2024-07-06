//go:build e2e

package mysql

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/require"
)

func (s *ServerTestSuite) TestPrepareStatement() {
	db, err := newDB()
	require.NoError(s.T(), err)

	ctx := context.Background()

	stmt, err := db.PrepareContext(ctx, "SELECT * FROM users WHERE ID = ?")

	if err != nil {
		return
	}
	defer func() {
		err := stmt.Close()
		if err != nil {
			s.T().Fatalf("Failed to close prepare: %v", err)
		}
	}()
	//if err != nil {
	//	s.T().Fatalf("Failed to prepare: %v", err)
	//}

	rows, err := stmt.QueryContext(ctx, 1)
	defer func() {
		err := rows.Close()
		if err != nil {
			s.T().Fatalf("Failed to close rows: %v", err)
		}
	}()
	for rows.Next() {
		//在这里读取并且打印数据
		//假设你只有 id 和 name 两个列
		var id int64
		var name string
		err = rows.Scan(&id, &name)
		require.NoError(s.T(), err)
		s.T().Log(id, name)
	}

}

func (s *ServerTestSuite) TestPrepareStatementReal() {
	db, err := sql.Open("mysql", "root:123456@tcp(120.76.42.33:3306)/test")
	require.NoError(s.T(), err)

	ctx := context.Background()

	stmt, err := db.PrepareContext(ctx, "SELECT * FROM users WHERE ID = ?")

	if err != nil {
		return
	}
	defer func() {
		err := stmt.Close()
		if err != nil {
			s.T().Fatalf("Failed to close prepare: %v", err)
		}
	}()
	if err != nil {
		s.T().Fatalf("Failed to prepare: %v", err)
	}

	rows, err := stmt.QueryContext(ctx, 1)
	defer func() {
		err := rows.Close()
		if err != nil {
			s.T().Fatalf("Failed to close rows: %v", err)
		}
	}()
	for rows.Next() {
		// 在这里读取并且打印数据
		// 假设你只有 id 和 name 两个列
		var id int64
		var name string
		err = rows.Scan(&id, &name)
		require.NoError(s.T(), err)
		s.T().Log(id, name)
	}

}
