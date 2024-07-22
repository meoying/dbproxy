package testsuite

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/driver/sharding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type DistributeTXTestSuite struct {
	suite.Suite
	db *sql.DB
	// shardingPluginUsedOnlyClientID 用于不同客户端并发执行事务时的测试数据隔离
	shardingPluginUsedOnlyClientID int
}

func (s *DistributeTXTestSuite) SetClientID(cid int) {
	s.shardingPluginUsedOnlyClientID = cid
}

func (s *DistributeTXTestSuite) SetDB(db *sql.DB) {
	s.db = db
}

func (s *DistributeTXTestSuite) TestDelayTransaction() {
	t := s.T()
	testcases := []struct {
		name                  string
		before                func(t *testing.T)
		driverUsedOnlyCtxFunc func() context.Context
		sqlStmts              []string
		execSQLStmts          func(t *testing.T, sqlStmts []string, tx *sql.Tx)
		after                 func(t *testing.T)
	}{
		{
			name:                  "插入操作_提交事务",
			before:                func(t *testing.T) {},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{
				fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1001, 'sample content', 10.0);", s.getUserID(30001)),
				fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 2002, 'sample content', 20.0);", s.getUserID(30002)),
			},
			execSQLStmts: s.execSQLStmtsAndCommit,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{int64(s.getUserID(30001)), int64(s.getUserID(30002))})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(30001),
						OrderId: 1001,
						Content: "sample content",
						Account: 10.0,
					},
					{
						UserId:  s.getUserID(30002),
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
			name:                  "插入操作_回滚事务",
			before:                func(t *testing.T) {},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{
				fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1001, 'sample content', 10.0);", s.getUserID(40001)),
				fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 2002, 'sample content', 20.0);", s.getUserID(40002)),
			},
			execSQLStmts: s.execSQLStmtsAndRollback,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{int64(s.getUserID(40001))})
				orders := getOrdersFromRows(t, rows)
				assert.Equal(t, 0, len(orders))
			},
		},
		{
			name: "读取操作_提交事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1002, 'initial content', 20.0)", s.getUserID(50002)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{
				fmt.Sprintf("SELECT /*useMaster*/ `content` FROM `order` WHERE `user_id` = %d;", s.getUserID(50002)),
			},
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
				rows := getRowsFromTable(t, s.db, []int64{int64(s.getUserID(50002))})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(50002),
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
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1002, 'initial content', 20.0)", s.getUserID(60002)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{
				fmt.Sprintf("SELECT /*useMaster*/ `content` FROM `order` WHERE `user_id` = %d", s.getUserID(60002)),
			},
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
					fmt.Sprintf("INSERT INTO `order`(`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1002, 'initial content', 20.0)", s.getUserID(70002)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1102, 'initial content', 120.0)", s.getUserID(700012)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1112, 'initial content', 1120.0)", s.getUserID(7000112)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{
				fmt.Sprintf("UPDATE `order` SET `content` = 'updated content' WHERE `user_id` in (%d,%d,%d)",
					s.getUserID(70002),
					s.getUserID(700012),
					s.getUserID(7000112),
				),
			},
			execSQLStmts: s.execSQLStmtsAndCommit,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(70002)),
					int64(s.getUserID(700012)),
					int64(s.getUserID(7000112)),
				})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(70002),
						OrderId: 1002,
						Content: "updated content",
						Account: 20.0,
					},
					{
						UserId:  s.getUserID(700012),
						OrderId: 1102,
						Content: "updated content",
						Account: 120.0,
					},
					{
						UserId:  s.getUserID(7000112),
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
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1002, 'initial content', 20.0)", s.getUserID(80002)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1102, 'initial content', 120.0)", s.getUserID(800012)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1112, 'initial content', 1120.0)", s.getUserID(8000112)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{
				fmt.Sprintf("UPDATE `order` SET `content` = 'updated content' WHERE (`user_id` = %d) OR (`user_id` = %d) OR (`user_id` = %d);",
					s.getUserID(80002),
					s.getUserID(800012),
					s.getUserID(8000112),
				),
			},
			execSQLStmts: s.execSQLStmtsAndRollback,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(80002)),
					int64(s.getUserID(800012)),
					int64(s.getUserID(8000112)),
				})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(80002),
						OrderId: 1002,
						Content: "initial content",
						Account: 20.0,
					},
					{
						UserId:  s.getUserID(800012),
						OrderId: 1102,
						Content: "initial content",
						Account: 120.0,
					},
					{
						UserId:  s.getUserID(8000112),
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
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1003, 'delete content', 30.0)", s.getUserID(90003)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1103, 'delete content', 130.0)", s.getUserID(900013)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1113, 'delete content', 1130.0)", s.getUserID(9000113)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{
				fmt.Sprintf("DELETE FROM `order` WHERE `user_id` in (%d, %d, %d)",
					s.getUserID(90003),
					s.getUserID(900013),
					s.getUserID(9000113),
				),
			},
			execSQLStmts: s.execSQLStmtsAndCommit,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(90003)),
					int64(s.getUserID(900013)),
					int64(s.getUserID(9000113)),
				})
				orders := getOrdersFromRows(t, rows)
				assert.Equal(t, 0, len(orders))
			},
		},
		{
			name: "删除操作_回滚事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1003, 'delete content', 30.0)", s.getUserID(100009)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1103, 'delete content', 130.0)", s.getUserID(1000019)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1113, 'delete content', 1130.0)", s.getUserID(10000119)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{
				fmt.Sprintf("DELETE FROM `order` WHERE `user_id` in (%d, %d, %d)",
					s.getUserID(100009),
					s.getUserID(1000019),
					s.getUserID(10000119),
				),
			},
			execSQLStmts: s.execSQLStmtsAndRollback,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(100009)),
					int64(s.getUserID(1000019)),
					int64(s.getUserID(10000119)),
				})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(100009),
						OrderId: 1003,
						Content: "delete content",
						Account: 30.0,
					},
					{
						UserId:  s.getUserID(1000019),
						OrderId: 1103,
						Content: "delete content",
						Account: 130.0,
					},
					{
						UserId:  s.getUserID(10000119),
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
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 2002, 'initial content', 220.0)", s.getUserID(2000022)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 2003, 'delete content', 230.0)", s.getUserID(2000023)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{
				fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 2004, 'insert content', 240.0);", s.getUserID(2000024)),
				fmt.Sprintf("UPDATE `order` SET `content` = 'updated content again' WHERE `user_id` = %d;", s.getUserID(2000022)),
				fmt.Sprintf("DELETE FROM `order` WHERE `user_id` = %d;", s.getUserID(2000023)),
			},
			execSQLStmts: s.execSQLStmtsAndCommit,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(2000022)),
					int64(s.getUserID(2000023)),
					int64(s.getUserID(2000024)),
				})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(2000022),
						OrderId: 2002,
						Content: "updated content again",
						Account: 220.0,
					},
					{
						UserId:  s.getUserID(2000024),
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
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 2002, 'initial content', 220.0)", s.getUserID(3000022)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 2003, 'delete content', 230.0)", s.getUserID(3000023)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewDelayTxContext(context.Background()) },
			sqlStmts: []string{
				fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 2005, 'rollback insert content', 250.0);", s.getUserID(3000025)),
				fmt.Sprintf("UPDATE `order` SET `content` = 'rollback update content' WHERE `user_id` = %d;", s.getUserID(3000022)),
				fmt.Sprintf("DELETE FROM `order` WHERE `user_id` = %d;", s.getUserID(3000023)),
			},
			execSQLStmts: s.execSQLStmtsAndRollback,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(3000022)),
					int64(s.getUserID(3000023)),
				})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(3000022),
						OrderId: 2002,
						Content: "initial content",
						Account: 220.0,
					},
					{
						UserId:  s.getUserID(3000023),
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
			tx, err := s.db.BeginTx(tc.driverUsedOnlyCtxFunc(), nil)
			require.NoError(t, err)

			// 在事务tx中执行SQL语句
			tc.execSQLStmts(t, tc.sqlStmts, tx)

			// 验证结果, 使用s.db验证执行tc.sqlStmt后的影响
			tc.after(t)

			// 因并发测试的存在,所以不会清理表中所有数据
		})
	}

}

func (s *DistributeTXTestSuite) getUserID(uid int) int {
	return uid + s.shardingPluginUsedOnlyClientID
}

func (s *DistributeTXTestSuite) execSQLStmtsAndCommit(t *testing.T, sqlStmts []string, tx *sql.Tx) {
	t.Helper()
	for _, sqlStmt := range sqlStmts {
		_, err := tx.Exec(sqlStmt)
		assert.NoError(t, err)
	}
	assert.NoError(t, tx.Commit())
}

func (s *DistributeTXTestSuite) execSQLStmtsAndRollback(t *testing.T, sqlStmts []string, tx *sql.Tx) {
	t.Helper()
	for _, sqlStmt := range sqlStmts {
		_, err := tx.Exec(sqlStmt)
		require.NoError(t, err)
	}
	err := tx.Rollback()
	require.NoError(t, err)
}

func (s *DistributeTXTestSuite) TestDelayTransactionErr() {
	t := s.T()
	tx, err := s.db.BeginTx(sharding.NewDelayTxContext(context.Background()), nil)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.ErrorIs(t, tx.Commit(), sql.ErrTxDone)
	require.ErrorIs(t, tx.Rollback(), sql.ErrTxDone)
}
