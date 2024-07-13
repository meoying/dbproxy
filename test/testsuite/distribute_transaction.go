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
			sqlStmts: []string{"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (30001, 1001, 'sample content', 10.0),(30002, 2002, 'sample content', 20.0);"},
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
				rows := getRowsFromTable(t, s.db, []int64{30001, 30002})
				wantOrderList := []Order{
					{
						UserId:  30001,
						OrderId: 1001,
						Content: "sample content",
						Account: 10.0,
					},
					{
						UserId:  30002,
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
			sqlStmts: []string{"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (40001, 1001, 'sample content', 10.0),(40002, 2002, 'sample content', 20.0);"},
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
				rows := getRowsFromTable(t, s.db, []int64{40001})
				orders := getOrdersFromRows(t, rows)
				assert.Equal(t, 0, len(orders))
			},
		},
		{
			name: "读取操作_提交事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (50002, 1002, 'initial content', 20.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc:  func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{"SELECT /*useMaster*/ `content` FROM `order` WHERE `user_id` = 50002;"},
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
				rows := getRowsFromTable(t, s.db, []int64{50002})
				wantOrderList := []Order{
					{
						UserId:  50002,
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
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (60002, 1002, 'initial content', 20.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc:  func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{"SELECT /*useMaster*/ `content` FROM `order` WHERE `user_id` = 60002"},
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
					"INSERT INTO `order`(`user_id`, `order_id`, `content`, `account`) VALUES (70002, 1002, 'initial content', 20.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (700012, 1102, 'initial content', 120.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (7000112, 1112, 'initial content', 1120.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc:  func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{"UPDATE `order` SET `content` = 'updated content' WHERE `user_id` in (70002,700012,7000112)"},
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
				rows := getRowsFromTable(t, s.db, []int64{70002, 700012, 7000112})
				wantOrderList := []Order{
					{
						UserId:  70002,
						OrderId: 1002,
						Content: "updated content",
						Account: 20.0,
					},
					{
						UserId:  700012,
						OrderId: 1102,
						Content: "updated content",
						Account: 120.0,
					},
					{
						UserId:  7000112,
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
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (80002, 1002, 'initial content', 20.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (800012, 1102, 'initial content', 120.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (8000112, 1112, 'initial content', 1120.0)",
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
				rows := getRowsFromTable(t, s.db, []int64{80002, 800012, 8000112})
				wantOrderList := []Order{
					{
						UserId:  80002,
						OrderId: 1002,
						Content: "initial content",
						Account: 20.0,
					},
					{
						UserId:  800012,
						OrderId: 1102,
						Content: "initial content",
						Account: 120.0,
					},
					{
						UserId:  8000112,
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
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (90003, 1003, 'delete content', 30.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (900013, 1103, 'delete content', 130.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (9000113, 1113, 'delete content', 1130.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc:  func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{"DELETE FROM `order` WHERE `user_id` in (90003,900013,9000113)"},
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
				rows := getRowsFromTable(t, s.db, []int64{90003, 900013, 9000113})
				orders := getOrdersFromRows(t, rows)
				assert.Equal(t, 0, len(orders))
			},
		},
		{
			name: "删除操作_回滚事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (100009, 1003, 'delete content', 30.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (1000019, 1103, 'delete content', 130.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (10000119, 1113, 'delete content', 1130.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc:  func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{"DELETE FROM `order` WHERE `user_id` in (100009, 1000019, 10000119)"},
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
				rows := getRowsFromTable(t, s.db, []int64{100009, 1000019, 10000119})
				wantOrderList := []Order{
					{
						UserId:  100009,
						OrderId: 1003,
						Content: "delete content",
						Account: 30.0,
					},
					{
						UserId:  1000019,
						OrderId: 1103,
						Content: "delete content",
						Account: 130.0,
					},
					{
						UserId:  10000119,
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
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (2000022, 2002, 'initial content', 220.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (2000023, 2003, 'delete content', 230.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc: func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{
				"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (2000024, 2004, 'insert content', 240.0);",
				"UPDATE `order` SET `content` = 'updated content again' WHERE `user_id` = 2000022;",
				"DELETE FROM `order` WHERE `user_id` = 2000023;",
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
				rows := getRowsFromTable(t, s.db, []int64{2000022, 2000023, 2000024})
				wantOrderList := []Order{
					{
						UserId:  2000022,
						OrderId: 2002,
						Content: "updated content again",
						Account: 220.0,
					},
					{
						UserId:  2000024,
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
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (3000022, 2002, 'initial content', 220.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (3000023, 2003, 'delete content', 230.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc: func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{
				"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (3000025, 2005, 'rollback insert content', 250.0);",
				"UPDATE `order` SET `content` = 'rollback update content' WHERE `user_id` = 3000022;",
				"DELETE FROM `order` WHERE `user_id` = 3000023;",
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
				rows := getRowsFromTable(t, s.db, []int64{3000022, 3000023})
				wantOrderList := []Order{
					{
						UserId:  3000022,
						OrderId: 2002,
						Content: "initial content",
						Account: 220.0,
					},
					{
						UserId:  3000023,
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
