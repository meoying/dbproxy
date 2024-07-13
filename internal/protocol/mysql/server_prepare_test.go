//go:build e2e

package mysql

import (
	"context"
	"github.com/stretchr/testify/require"
)

func (s *ServerTestSuite) TestPrepareStatement() {
	t := s.T()
	db, err := newDB()
	require.NoError(t, err)

	ctx := context.Background()

	stmt, err := db.PrepareContext(ctx, "SELECT * FROM users WHERE ID = ?")
	require.NoError(t, err)

	rows, err := stmt.QueryContext(ctx, 1)
	require.NoError(t, err)
	for rows.Next() {
		//在这里读取并且打印数据
		//假设你只有 id 和 name 两个列
		var id int64
		var name string
		err = rows.Scan(&id, &name)
		require.NoError(t, err)
		t.Log(id, name)
	}

	err = rows.Close()
	require.NoError(t, err)

	err = stmt.Close()
	require.NoError(t, err)

}
