package testsuite

import (
	"context"
	"database/sql"
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/driver/sharding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type SingleTXTestSuite struct {
	suite.Suite
	db *sql.DB
}

func (s *SingleTXTestSuite) SetDB(db *sql.DB) {
	s.db = db
}

// TestLocalTransaction 测试单机(节点)本地事务
func (s *SingleTXTestSuite) TestLocalTransaction() {
	t := s.T()

	testcases := []struct {
		name         string
		before       func(t *testing.T)
		ctxFunc      func() context.Context
		sqlStmts     []string
		execSQLStmts func(t *testing.T, sqlStmts []string, tx *sql.Tx)
		after        func(t *testing.T)
	}{
		{
			name:     "插入操作_提交事务",
			before:   func(t *testing.T) {},
			ctxFunc:  func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (1001, 1001, 'sample content', 10.0);"},
			execSQLStmts: func(t *testing.T, sqlStmts []string, tx *sql.Tx) {
				t.Helper()
				for _, sqlStmt := range sqlStmts {
					_, err := tx.Exec(sqlStmt)
					require.NoError(t, err)
				}
				err := tx.Commit()
				require.NoError(t, err)
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{1001})
				wantOrderList := []Order{
					{
						UserId:  1001,
						OrderId: 1001,
						Content: "sample content",
						Account: 10.0,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		{
			name:     "插入操作_回滚事务",
			before:   func(t *testing.T) {},
			ctxFunc:  func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (1002, 1001, 'abc_sample content', 10.0)"},
			execSQLStmts: func(t *testing.T, sqlStmts []string, tx *sql.Tx) {
				t.Helper()
				for _, sqlStmt := range sqlStmts {
					_, err := tx.Exec(sqlStmt)
					require.NoError(t, err)
				}
				err := tx.Rollback()
				require.NoError(t, err)
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{1})
				orders := getOrdersFromRows(t, rows)
				assert.Equal(t, 0, len(orders))
			},
		},
		{
			name: "读取操作_提交事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (2002, 1002, 'initial content', 20.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc:  func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{"SELECT /*useMaster*/ `content` FROM `order` WHERE `user_id` = 2002;"},
			execSQLStmts: func(t *testing.T, sqlStmts []string, tx *sql.Tx) {
				t.Helper()
				sqlStmt := sqlStmts[0]
				var content string
				err := tx.QueryRow(sqlStmt).Scan(&content)
				require.NoError(t, err)
				require.Equal(t, "initial content", content)
				err = tx.Commit()
				require.NoError(t, err)
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{2002})
				wantOrderList := []Order{
					{
						UserId:  2002,
						OrderId: 1002,
						Content: "initial content",
						Account: 20.0,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		{
			name: "读取操作_回滚事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (2003, 1002, 'initial content', 20.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc:  func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{"SELECT /*useMaster*/ `content` FROM `order` WHERE `user_id` = 2003"},
			execSQLStmts: func(t *testing.T, sqlStmts []string, tx *sql.Tx) {
				t.Helper()
				sqlStmt := sqlStmts[0]
				var content string
				err := tx.QueryRow(sqlStmt).Scan(&content)
				require.NoError(t, err)
				require.Equal(t, "initial content", content)
				err = tx.Rollback()
				require.NoError(t, err)
			},
			after: func(t *testing.T) {},
		},
		{
			name: "更新操作_提交事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (3003, 1002, 'initial content', 20.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc:  func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{"UPDATE `order` SET `content` = 'updated content' WHERE `user_id` = 3003;"},
			execSQLStmts: func(t *testing.T, sqlStmts []string, tx *sql.Tx) {
				t.Helper()
				for _, sqlStmt := range sqlStmts {
					_, err := tx.Exec(sqlStmt)
					require.NoError(t, err)
				}
				err := tx.Commit()
				require.NoError(t, err)
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{3003})
				wantOrderList := []Order{
					{
						UserId:  3003,
						OrderId: 1002,
						Content: "updated content",
						Account: 20.0,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		{
			name: "更新操作_回滚事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (42, 1002, 'initial content', 20.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (412, 1102, 'initial content', 120.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (4112, 1112, 'initial content', 1120.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc:  func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{"UPDATE `order` SET `content` = 'updated content' WHERE (`user_id` = 42) OR (`user_id` = 412);"},
			execSQLStmts: func(t *testing.T, sqlStmts []string, tx *sql.Tx) {
				t.Helper()
				for _, sqlStmt := range sqlStmts {
					_, err := tx.Exec(sqlStmt)
					require.NoError(t, err)
				}
				err := tx.Rollback()
				require.NoError(t, err)
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{42, 412, 4112})
				wantOrderList := []Order{
					{
						UserId:  42,
						OrderId: 1002,
						Content: "initial content",
						Account: 20.0,
					},
					{
						UserId:  412,
						OrderId: 1102,
						Content: "initial content",
						Account: 120.0,
					},
					{
						UserId:  4112,
						OrderId: 1112,
						Content: "initial content",
						Account: 1120.0,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		{
			name: "删除操作_提交事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (5113, 1113, 'delete content', 1130.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc:  func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{"DELETE FROM `order` WHERE `user_id` = 5113;"},
			execSQLStmts: func(t *testing.T, sqlStmts []string, tx *sql.Tx) {
				t.Helper()
				for _, sqlStmt := range sqlStmts {
					_, err := tx.Exec(sqlStmt)
					require.NoError(t, err)
				}
				err := tx.Commit()
				require.NoError(t, err)
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{5113})
				orders := getOrdersFromRows(t, rows)
				assert.Equal(t, 0, len(orders))
			},
		},
		{
			name: "删除操作_回滚事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (6119, 1003, 'delete content', 30.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (61119, 1103, 'delete content', 130.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (611119, 1113, 'delete content', 1130.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc:  func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{"DELETE FROM `order` WHERE `user_id` in (6119, 61119, 611119)"},
			execSQLStmts: func(t *testing.T, sqlStmts []string, tx *sql.Tx) {
				t.Helper()
				for _, sqlStmt := range sqlStmts {
					_, err := tx.Exec(sqlStmt)
					require.NoError(t, err)
				}
				err := tx.Rollback()
				require.NoError(t, err)
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{6119, 61119, 611119})
				wantOrderList := []Order{
					{
						UserId:  6119,
						OrderId: 1003,
						Content: "delete content",
						Account: 30.0,
					},
					{
						UserId:  61119,
						OrderId: 1103,
						Content: "delete content",
						Account: 130.0,
					},
					{
						UserId:  611119,
						OrderId: 1113,
						Content: "delete content",
						Account: 1130.0,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		{
			name: "组合操作_提交事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (7122, 2002, 'initial content', 220.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{
				"INSERT INTO `order` (user_id, order_id, content, account) VALUES (7125, 2005, 'insert content', 250.0);",
				"UPDATE `order` SET `content` = 'updated content again' WHERE `user_id` = 7122;",
			},
			execSQLStmts: func(t *testing.T, sqlStmts []string, tx *sql.Tx) {
				t.Helper()
				for _, sqlStmt := range sqlStmts {
					_, err := tx.Exec(sqlStmt)
					require.NoError(t, err)
				}
				err := tx.Commit()
				require.NoError(t, err)
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{7122, 7125})
				wantOrderList := []Order{
					{
						UserId:  7122,
						OrderId: 2002,
						Content: "updated content again",
						Account: 220.0,
					},
					{
						UserId:  7125,
						OrderId: 2005,
						Content: "insert content",
						Account: 250.0,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		{
			name: "组合操作_回滚事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (8222, 2002, 'initial content', 220.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (8223, 2003, 'delete content', 230.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{
				"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (8225, 2005, 'rollback insert content', 250.0);",
				"UPDATE `order` SET `content` = 'rollback update content' WHERE `user_id` = 8222;",
				"DELETE FROM `order` WHERE `user_id` = 8223;",
			},
			execSQLStmts: func(t *testing.T, sqlStmts []string, tx *sql.Tx) {
				t.Helper()
				for _, sqlStmt := range sqlStmts {
					_, err := tx.Exec(sqlStmt)
					require.NoError(t, err)
				}
				err := tx.Rollback()
				require.NoError(t, err)
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{8222, 8223})
				wantOrderList := []Order{
					{
						UserId:  8222,
						OrderId: 2002,
						Content: "initial content",
						Account: 220.0,
					},
					{
						UserId:  8223,
						OrderId: 2003,
						Content: "delete content",
						Account: 230.0,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// 准备测试数据
			tc.before(t)

			// 开启事务
			tx, err := s.db.BeginTx(tc.ctxFunc(), nil)
			require.NoError(t, err)

			// 在事务tx中执行SQL语句
			tc.execSQLStmts(t, tc.sqlStmts, tx)

			// 验证结果, 使用s.db验证执行tc.sqlStmt后的影响
			tc.after(t)

			// 清理数据
			ClearTables(t, s.db)
		})
	}
}
