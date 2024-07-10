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

type DistributeTXTestSuite struct {
	suite.Suite
	db *sql.DB
}

func (s *DistributeTXTestSuite) SetDB(db *sql.DB) {
	s.db = db
}

func (s *DistributeTXTestSuite) TestDelayTransaction() {
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
			ctxFunc:  func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (1, 1001, 'sample content', 10.0),(2, 2002, 'sample content', 20.0);"},
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
				rows := getRowsFromTable(t, s.db, []int64{1, 2})
				wantOrderList := []Order{
					{
						UserId:  1,
						OrderId: 1001,
						Content: "sample content",
						Account: 10.0,
					},
					{
						UserId:  2,
						OrderId: 2002,
						Content: "sample content",
						Account: 20.0,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		{
			name:     "插入操作_回滚事务",
			before:   func(t *testing.T) {},
			ctxFunc:  func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (1, 1001, 'sample content', 10.0),(2, 2002, 'sample content', 20.0);"},
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
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (2, 1002, 'initial content', 20.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc:  func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{"SELECT /*useMaster*/ `content` FROM `order` WHERE `user_id` = 2;"},
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
				rows := getRowsFromTable(t, s.db, []int64{2})
				wantOrderList := []Order{
					{
						UserId:  2,
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
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (2, 1002, 'initial content', 20.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc:  func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{"SELECT /*useMaster*/ `content` FROM `order` WHERE `user_id` = 2"},
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
					"INSERT INTO `order`(`user_id`, `order_id`, `content`, `account`) VALUES (2, 1002, 'initial content', 20.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (12, 1102, 'initial content', 120.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (112, 1112, 'initial content', 1120.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc:  func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{"UPDATE `order` SET `content` = 'updated content' WHERE `user_id` in (2,12,112)"},
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
				rows := getRowsFromTable(t, s.db, []int64{2, 12, 112})
				wantOrderList := []Order{
					{
						UserId:  2,
						OrderId: 1002,
						Content: "updated content",
						Account: 20.0,
					},
					{
						UserId:  12,
						OrderId: 1102,
						Content: "updated content",
						Account: 120.0,
					},
					{
						UserId:  112,
						OrderId: 1112,
						Content: "updated content",
						Account: 1120.0,
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
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (2, 1002, 'initial content', 20.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (12, 1102, 'initial content', 120.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (112, 1112, 'initial content', 1120.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc:  func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{"UPDATE `order` SET `content` = 'updated content' WHERE (`user_id` = 2) OR (`user_id` = 12);"},
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
				rows := getRowsFromTable(t, s.db, []int64{2, 12, 112})
				wantOrderList := []Order{
					{
						UserId:  2,
						OrderId: 1002,
						Content: "initial content",
						Account: 20.0,
					},
					{
						UserId:  12,
						OrderId: 1102,
						Content: "initial content",
						Account: 120.0,
					},
					{
						UserId:  112,
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
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (3, 1003, 'delete content', 30.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (13, 1103, 'delete content', 130.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (113, 1113, 'delete content', 1130.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc:  func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{"DELETE FROM `order` WHERE `user_id` in (3,13,113)"},
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
				rows := getRowsFromTable(t, s.db, []int64{3, 13, 113})
				orders := getOrdersFromRows(t, rows)
				assert.Equal(t, 0, len(orders))
			},
		},
		{
			name: "删除操作_回滚事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (9, 1003, 'delete content', 30.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (19, 1103, 'delete content', 130.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (119, 1113, 'delete content', 1130.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc:  func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{"DELETE FROM `order` WHERE `user_id` in (9, 19, 119)"},
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
				rows := getRowsFromTable(t, s.db, []int64{9, 19, 119})
				wantOrderList := []Order{
					{
						UserId:  9,
						OrderId: 1003,
						Content: "delete content",
						Account: 30.0,
					},
					{
						UserId:  19,
						OrderId: 1103,
						Content: "delete content",
						Account: 130.0,
					},
					{
						UserId:  119,
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
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (22, 2002, 'initial content', 220.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (23, 2003, 'delete content', 230.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc: func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{
				"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (24, 2004, 'insert content', 240.0);",
				"UPDATE `order` SET `content` = 'updated content again' WHERE `user_id` = 22;",
				"DELETE FROM `order` WHERE `user_id` = 23;",
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
				rows := getRowsFromTable(t, s.db, []int64{22, 23, 24})
				wantOrderList := []Order{
					{
						UserId:  22,
						OrderId: 2002,
						Content: "updated content again",
						Account: 220.0,
					},
					{
						UserId:  24,
						OrderId: 2004,
						Content: "insert content",
						Account: 240.0,
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
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (22, 2002, 'initial content', 220.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (23, 2003, 'delete content', 230.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc: func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{
				"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (25, 2005, 'rollback insert content', 250.0);",
				"UPDATE `order` SET `content` = 'rollback update content' WHERE `user_id` = 22;",
				"DELETE FROM `order` WHERE `user_id` = 23;",
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
				rows := getRowsFromTable(t, s.db, []int64{22, 23})
				wantOrderList := []Order{
					{
						UserId:  22,
						OrderId: 2002,
						Content: "initial content",
						Account: 220.0,
					},
					{
						UserId:  23,
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
			clearTable(t, s.db)
		})
	}

}
