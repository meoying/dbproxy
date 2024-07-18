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

type SingleTXTestSuite struct {
	suite.Suite
	db *sql.DB
	// forwardPluginUsedOnlyClientID 用于不同客户端并发执行事务时的测试数据隔离
	forwardPluginUsedOnlyClientID int
}

func (s *SingleTXTestSuite) SetDB(db *sql.DB) {
	s.db = db
}

func (s *SingleTXTestSuite) SetClientID(cid int) {
	s.forwardPluginUsedOnlyClientID = cid
}

// TestLocalTransaction 测试单机(节点)本地事务
func (s *SingleTXTestSuite) TestLocalTransaction() {
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
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{
				fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1001, 'sample content', 10.0);", s.getUserID(1001)),
			},
			execSQLStmts: s.execSQLStmtsAndCommit,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(1001)),
				})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(1001),
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
			name:                  "插入操作_回滚事务",
			before:                func(t *testing.T) {},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{
				fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1002, 'abc_sample content', 10.0)", s.getUserID(1002)),
			},
			execSQLStmts: s.execSQLStmtsAndRollback,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{int64(s.getUserID(1002))})
				orders := getOrdersFromRows(t, rows)
				assert.Equal(t, 0, len(orders))
			},
		},
		{
			name: "读取操作_提交事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 2002, 'initial content', 20.0)", s.getUserID(2002)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{
				fmt.Sprintf("SELECT /*useMaster*/ `content` FROM `order` WHERE `user_id` = %d;", s.getUserID(2002)),
			},
			execSQLStmts: func(t *testing.T, sqlStmts []string, tx *sql.Tx) {
				t.Helper()
				sqlStmt := sqlStmts[0]
				var content string
				rows, err := tx.Query(sqlStmt)
				require.NoError(t, err)
				for rows.Next() {
					err = rows.Scan(&content)
					assert.NoError(t, err)
				}
				assert.Equal(t, "initial content", content)
				assert.NoError(t, rows.Close())
				assert.NoError(t, tx.Commit())
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{int64(s.getUserID(2002))})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(2002),
						OrderId: 2002,
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
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 2003, 'initial content', 20.0)", s.getUserID(2003)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{
				fmt.Sprintf("SELECT /*useMaster*/ `content` FROM `order` WHERE `user_id` = %d", s.getUserID(2003)),
			},
			execSQLStmts: func(t *testing.T, sqlStmts []string, tx *sql.Tx) {
				t.Helper()
				sqlStmt := sqlStmts[0]
				var content string
				err := tx.QueryRow(sqlStmt).Scan(&content)
				assert.NoError(t, err)
				assert.Equal(t, "initial content", content)
				assert.NoError(t, tx.Rollback())
			},
			after: func(t *testing.T) {},
		},
		{
			name: "更新操作_提交事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 3003, 'initial content', 20.0)", s.getUserID(3003)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{
				fmt.Sprintf("UPDATE `order` SET `content` = 'updated content' WHERE `user_id` = %d;", s.getUserID(3003)),
			},
			execSQLStmts: s.execSQLStmtsAndCommit,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{int64(s.getUserID(3003))})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(3003),
						OrderId: 3003,
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
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1002, 'initial content', 20.0)", s.getUserID(42)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1102, 'initial content', 120.0)", s.getUserID(412)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1112, 'initial content', 1120.0)", s.getUserID(4112)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{
				fmt.Sprintf("UPDATE `order` SET `content` = 'updated content' WHERE (`user_id` = %d) OR (`user_id` = %d);",
					s.getUserID(42),
					s.getUserID(412),
				),
			},
			execSQLStmts: s.execSQLStmtsAndRollback,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(42)),
					int64(s.getUserID(412)),
					int64(s.getUserID(4112)),
				})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(42),
						OrderId: 1002,
						Content: "initial content",
						Account: 20.0,
					},
					{
						UserId:  s.getUserID(412),
						OrderId: 1102,
						Content: "initial content",
						Account: 120.0,
					},
					{
						UserId:  s.getUserID(4112),
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
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1113, 'delete content', 1130.0)", s.getUserID(5113)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{
				fmt.Sprintf("DELETE FROM `order` WHERE `user_id` = %d;", s.getUserID(5113)),
			},
			execSQLStmts: s.execSQLStmtsAndCommit,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{int64(s.getUserID(5113))})
				orders := getOrdersFromRows(t, rows)
				assert.Equal(t, 0, len(orders))
			},
		},
		{
			name: "删除操作_回滚事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1003, 'delete content', 30.0)", s.getUserID(6119)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1103, 'delete content', 130.0)", s.getUserID(61119)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1113, 'delete content', 1130.0)", s.getUserID(611119)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{
				fmt.Sprintf("DELETE FROM `order` WHERE `user_id` in (%d, %d, %d)", s.getUserID(6119), s.getUserID(61119), s.getUserID(611119)),
			},
			execSQLStmts: s.execSQLStmtsAndRollback,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{int64(s.getUserID(6119)), int64(s.getUserID(61119)), int64(s.getUserID(611119))})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(6119),
						OrderId: 1003,
						Content: "delete content",
						Account: 30.0,
					},
					{
						UserId:  s.getUserID(61119),
						OrderId: 1103,
						Content: "delete content",
						Account: 130.0,
					},
					{
						UserId:  s.getUserID(611119),
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

					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 2002, 'initial content', 220.0)", s.getUserID(7122)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{
				fmt.Sprintf("INSERT INTO `order` (user_id, order_id, content, account) VALUES (%d, 2005, 'insert content', 250.0);", s.getUserID(7125)),
				fmt.Sprintf("UPDATE `order` SET `content` = 'updated content again' WHERE `user_id` = %d;", s.getUserID(7122)),
			},
			execSQLStmts: s.execSQLStmtsAndCommit,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{int64(s.getUserID(7122)), int64(s.getUserID(7125))})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(7122),
						OrderId: 2002,
						Content: "updated content again",
						Account: 220.0,
					},
					{
						UserId:  s.getUserID(7125),
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
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 2002, 'initial content', 220.0)", s.getUserID(8222)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 2003, 'delete content', 230.0)", s.getUserID(8223)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{
				fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 2005, 'rollback insert content', 250.0);", s.getUserID(8225)),
				fmt.Sprintf("UPDATE `order` SET `content` = 'rollback update content' WHERE `user_id` = %d;", s.getUserID(8222)),
				fmt.Sprintf("DELETE FROM `order` WHERE `user_id` = %d;", s.getUserID(8223)),
			},
			execSQLStmts: s.execSQLStmtsAndRollback,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{int64(s.getUserID(8222)), int64(s.getUserID(8223))})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(8222),
						OrderId: 2002,
						Content: "initial content",
						Account: 220.0,
					},
					{
						UserId:  s.getUserID(8223),
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

			// 因并发测试的存在,所以不会清理数据
		})
	}
}

func (s *SingleTXTestSuite) getUserID(uid int) int {
	return uid + s.forwardPluginUsedOnlyClientID
}

func (s *SingleTXTestSuite) execSQLStmtsAndCommit(t *testing.T, sqlStmts []string, tx *sql.Tx) {
	t.Helper()
	for _, sqlStmt := range sqlStmts {
		_, err := tx.Exec(sqlStmt)
		assert.NoError(t, err)
	}
	assert.NoError(t, tx.Commit())
}

func (s *SingleTXTestSuite) execSQLStmtsAndRollback(t *testing.T, sqlStmts []string, tx *sql.Tx) {
	t.Helper()
	for _, sqlStmt := range sqlStmts {
		_, err := tx.Exec(sqlStmt)
		require.NoError(t, err)
	}
	err := tx.Rollback()
	require.NoError(t, err)
}
