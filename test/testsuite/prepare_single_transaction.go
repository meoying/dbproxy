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

type PrepareSingleTXTestSuite struct {
	suite.Suite
	db *sql.DB
	// forwardPluginUsedOnlyClientID 用于不同客户端并发执行事务时的测试数据隔离
	forwardPluginUsedOnlyClientID int
}

func (s *PrepareSingleTXTestSuite) SetDB(db *sql.DB) {
	s.db = db
}

func (s *PrepareSingleTXTestSuite) SetClientID(cid int) {
	s.forwardPluginUsedOnlyClientID = cid
}

// TestLocalTransaction 测试单机(节点)本地事务
func (s *PrepareSingleTXTestSuite) TestLocalTransaction() {
	t := s.T()
	testcases := []struct {
		name                  string
		before                func(t *testing.T)
		driverUsedOnlyCtxFunc func() context.Context
		infos                 []sqlInfo
		execSQLStmts          func(t *testing.T, infos []sqlInfo, tx *sql.Tx)
		after                 func(t *testing.T)
	}{

		// 无占位符_插入多行_提交事务
		{
			name:                  "无占位符_插入多行_提交事务",
			before:                func(t *testing.T) {},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			infos: []sqlInfo{
				{
					query: fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 1001, 'sample content1001', 1001.0),(%d, 1002, 'sample content1002', 1002.0),(%d, 1003, 'sample content1003', 1003.0);",
						s.getUserID(1001), s.getUserID(1002), s.getUserID(1003)),
					rowsAffected: 3,
				},
			},
			execSQLStmts: s.execSQLStmtsAndCommit,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(1001)),
					int64(s.getUserID(1002)),
					int64(s.getUserID(1003)),
				})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(1001),
						OrderId: 1001,
						Content: "sample content1001",
						Account: 1001.0,
					},
					{
						UserId:  s.getUserID(1002),
						OrderId: 1002,
						Content: "sample content1002",
						Account: 1002.0,
					},
					{
						UserId:  s.getUserID(1003),
						OrderId: 1003,
						Content: "sample content1003",
						Account: 1003.0,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		// 有占位符_插入多行_提交事务
		{
			name:                  "有占位符_插入多行_提交事务",
			before:                func(t *testing.T) {},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			infos: []sqlInfo{
				{
					query: "INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (?, ?, ?, ?),(?, ?, ?, ?),(?, ?, ?, ?);",
					args: []any{
						s.getUserID(2001), 2001, "sample content2001", 2001.0,
						s.getUserID(2002), 2002, "sample content2002", 2002.0,
						s.getUserID(2003), 2003, "sample content2003", 2003.0,
					},
					rowsAffected: 3,
				},
			},
			execSQLStmts: s.execSQLStmtsAndCommit,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(2001)),
					int64(s.getUserID(2002)),
					int64(s.getUserID(2003)),
				})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(2001),
						OrderId: 2001,
						Content: "sample content2001",
						Account: 2001.0,
					},
					{
						UserId:  s.getUserID(2002),
						OrderId: 2002,
						Content: "sample content2002",
						Account: 2002.0,
					},
					{
						UserId:  s.getUserID(2003),
						OrderId: 2003,
						Content: "sample content2003",
						Account: 2003.0,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		// 无占位符_插入多行_回滚事务
		{
			name:                  "无占位符_插入多行_回滚事务",
			before:                func(t *testing.T) {},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			infos: []sqlInfo{
				{
					query: fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 3001, 'sample content3001', 3001.0),(%d, 3002, 'sample content3002', 3002.0),(%d, 3003, 'sample content3003', 3003.0);",
						s.getUserID(3001), s.getUserID(3002), s.getUserID(3003)),
					rowsAffected: 3,
				},
			},
			execSQLStmts: s.execSQLStmtsAndRollback,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(3001)),
					int64(s.getUserID(3002)),
					int64(s.getUserID(3003)),
				})
				orders := getOrdersFromRows(t, rows)
				assert.Equal(t, 0, len(orders))
			},
		},
		// 有占位符_插入多行_回滚事务
		{
			name:                  "有占位符_插入多行_回滚事务",
			before:                func(t *testing.T) {},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			infos: []sqlInfo{
				{
					query: "INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (?, ?, ?, ?),(?, ?, ?, ?),(?, ?, ?, ?);",
					args: []any{
						s.getUserID(4001), 4001, "sample content4001", 4001.0,
						s.getUserID(4002), 4002, "sample content4002", 4002.0,
						s.getUserID(4003), 4003, "sample content4003", 4003.0,
					},
					rowsAffected: 3,
				},
			},
			execSQLStmts: s.execSQLStmtsAndRollback,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(4001)),
					int64(s.getUserID(4002)),
					int64(s.getUserID(4003)),
				})
				orders := getOrdersFromRows(t, rows)
				assert.Equal(t, 0, len(orders))
			},
		},

		// 无占位符_查询多行_提交事务
		{
			name: "无占位符_查询多行_提交事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 5001, 'initial content5001', 5001.0),(%d, 5002, 'initial content5002', 5002.0),(%d, 5003, 'initial content5003', 5003.0)",
						s.getUserID(5001), s.getUserID(5002), s.getUserID(5003)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			infos: []sqlInfo{
				{
					query: fmt.Sprintf("SELECT /*useMaster*/ `content` FROM `order` WHERE `user_id` IN (%d, %d, %d)", s.getUserID(5001), s.getUserID(5002), s.getUserID(5003)),
				},
			},
			execSQLStmts: func(t *testing.T, infos []sqlInfo, tx *sql.Tx) {
				t.Helper()
				var content string
				contents := make([]string, 0, 3)
				for _, info := range infos {

					stmt, err := tx.Prepare(info.query)
					require.NoError(t, err)

					rows, err := stmt.Query(info.args...)
					require.NoError(t, err)

					for rows.Next() {
						err = rows.Scan(&content)
						assert.NoError(t, err)
						contents = append(contents, content)
					}
					assert.NoError(t, rows.Close())
					assert.NoError(t, tx.Commit())
				}
				expected := []string{
					"initial content5001",
					"initial content5002",
					"initial content5003",
				}
				assert.ElementsMatch(t, expected, contents)
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(5001)),
					int64(s.getUserID(5002)),
					int64(s.getUserID(5003)),
				})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(5001),
						OrderId: 5001,
						Content: "initial content5001",
						Account: 5001.0,
					},
					{
						UserId:  s.getUserID(5002),
						OrderId: 5002,
						Content: "initial content5002",
						Account: 5002.0,
					},
					{
						UserId:  s.getUserID(5003),
						OrderId: 5003,
						Content: "initial content5003",
						Account: 5003.0,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		// 有占位符_查询多行_提交事务
		{
			name: "有占位符_查询多行_提交事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 6001, 'initial content6001', 6001.0),(%d, 6002, 'initial content6002', 6002.0),(%d, 6003, 'initial content6003', 6003.0)",
						s.getUserID(6001), s.getUserID(6002), s.getUserID(6003)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			infos: []sqlInfo{
				{
					query: "SELECT /*useMaster*/ `content` FROM `order` WHERE `user_id` IN (?,?,?)",
					args: []any{
						s.getUserID(6001), s.getUserID(6002), s.getUserID(6003),
					},
				},
			},
			execSQLStmts: func(t *testing.T, infos []sqlInfo, tx *sql.Tx) {
				t.Helper()
				var content string
				contents := make([]string, 0, 3)
				for _, info := range infos {

					stmt, err := tx.Prepare(info.query)
					require.NoError(t, err)

					rows, err := stmt.Query(info.args...)
					require.NoError(t, err)

					for rows.Next() {
						err = rows.Scan(&content)
						assert.NoError(t, err)
						contents = append(contents, content)
					}
					assert.NoError(t, rows.Close())
					assert.NoError(t, tx.Commit())
				}
				expected := []string{
					"initial content6001",
					"initial content6002",
					"initial content6003",
				}
				assert.ElementsMatch(t, expected, contents)
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(6001)),
					int64(s.getUserID(6002)),
					int64(s.getUserID(6003)),
				})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(6001),
						OrderId: 6001,
						Content: "initial content6001",
						Account: 6001.0,
					},
					{
						UserId:  s.getUserID(6002),
						OrderId: 6002,
						Content: "initial content6002",
						Account: 6002.0,
					},
					{
						UserId:  s.getUserID(6003),
						OrderId: 6003,
						Content: "initial content6003",
						Account: 6003.0,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		// 无占位符_查询多行_回滚事务
		{
			name: "无占位符_查询多行_回滚事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 7001, 'initial content7001', 7001.0),(%d, 7002, 'initial content7002', 7002.0),(%d, 7003, 'initial content7003', 7003.0)",
						s.getUserID(7001), s.getUserID(7002), s.getUserID(7003)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			infos: []sqlInfo{
				{
					query: fmt.Sprintf("SELECT /*useMaster*/ `content` FROM `order` WHERE `user_id` IN (%d, %d, %d)", s.getUserID(7001), s.getUserID(7002), s.getUserID(7003)),
				},
			},
			execSQLStmts: func(t *testing.T, infos []sqlInfo, tx *sql.Tx) {
				t.Helper()
				var content string
				contents := make([]string, 0, 3)
				for _, info := range infos {

					stmt, err := tx.Prepare(info.query)
					require.NoError(t, err)

					rows, err := stmt.Query(info.args...)
					require.NoError(t, err)

					for rows.Next() {
						err = rows.Scan(&content)
						assert.NoError(t, err)
						contents = append(contents, content)
					}
					assert.NoError(t, rows.Close())
					assert.NoError(t, tx.Rollback())
				}
				expected := []string{
					"initial content7001",
					"initial content7002",
					"initial content7003",
				}
				assert.ElementsMatch(t, expected, contents)
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(7001)),
					int64(s.getUserID(7002)),
					int64(s.getUserID(7003)),
				})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(7001),
						OrderId: 7001,
						Content: "initial content7001",
						Account: 7001.0,
					},
					{
						UserId:  s.getUserID(7002),
						OrderId: 7002,
						Content: "initial content7002",
						Account: 7002.0,
					},
					{
						UserId:  s.getUserID(7003),
						OrderId: 7003,
						Content: "initial content7003",
						Account: 7003.0,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		// 有占位符_查询多行_回滚事务
		{
			name: "有占位符_查询多行_回滚事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 8001, 'initial content8001', 8001.0),(%d, 8002, 'initial content8002', 8002.0),(%d, 8003, 'initial content8003', 8003.0)",
						s.getUserID(8001), s.getUserID(8002), s.getUserID(8003)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			infos: []sqlInfo{
				{
					query: "SELECT /*useMaster*/ `content` FROM `order` WHERE `user_id` IN (?,?,?)",
					args: []any{
						s.getUserID(8001), s.getUserID(8002), s.getUserID(8003),
					},
				},
			},
			execSQLStmts: func(t *testing.T, infos []sqlInfo, tx *sql.Tx) {
				t.Helper()
				var content string
				contents := make([]string, 0, 3)
				for _, info := range infos {

					stmt, err := tx.Prepare(info.query)
					require.NoError(t, err)

					rows, err := stmt.Query(info.args...)
					require.NoError(t, err)

					for rows.Next() {
						err = rows.Scan(&content)
						assert.NoError(t, err)
						contents = append(contents, content)
					}
					assert.NoError(t, rows.Close())
					assert.NoError(t, tx.Rollback())
				}
				expected := []string{
					"initial content8001",
					"initial content8002",
					"initial content8003",
				}
				assert.ElementsMatch(t, expected, contents)
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(8001)),
					int64(s.getUserID(8002)),
					int64(s.getUserID(8003)),
				})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(8001),
						OrderId: 8001,
						Content: "initial content8001",
						Account: 8001.0,
					},
					{
						UserId:  s.getUserID(8002),
						OrderId: 8002,
						Content: "initial content8002",
						Account: 8002.0,
					},
					{
						UserId:  s.getUserID(8003),
						OrderId: 8003,
						Content: "initial content8003",
						Account: 8003.0,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},

		// 无占位符_更新多行_提交事务
		{
			name: "无占位符_更新多行_提交事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 9001, 'initial content9001', 9001.0)", s.getUserID(9001)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 9002, 'initial content9002', 9002.0)", s.getUserID(9002)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 9003, 'initial content9003', 9003.0)", s.getUserID(9003)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			infos: []sqlInfo{
				{
					query: fmt.Sprintf("UPDATE `order` SET `content` = 'updated content' WHERE `user_id` IN (%d, %d, %d);",
						s.getUserID(9001), s.getUserID(9002), s.getUserID(9003)),
					rowsAffected: 3,
				},
			},
			execSQLStmts: s.execSQLStmtsAndCommit,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(9001)),
					int64(s.getUserID(9002)),
					int64(s.getUserID(9003)),
				})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(9001),
						OrderId: 9001,
						Content: "updated content",
						Account: 9001.0,
					},
					{
						UserId:  s.getUserID(9002),
						OrderId: 9002,
						Content: "updated content",
						Account: 9002.0,
					},
					{
						UserId:  s.getUserID(9003),
						OrderId: 9003,
						Content: "updated content",
						Account: 9003.0,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		// 有占位符_更新多行_提交事务
		{
			name: "有占位符_更新多行_提交事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 10001, 'initial content10001', 10001.0)", s.getUserID(10001)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 10002, 'initial content10002', 10002.0)", s.getUserID(10002)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 10003, 'initial content10003', 10003.0)", s.getUserID(10003)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			infos: []sqlInfo{
				{
					query: "UPDATE `order` SET `content` = 'updated content' WHERE `user_id` IN (?,?,?);",
					args: []any{
						s.getUserID(10001), s.getUserID(10002), s.getUserID(10003),
					},
					rowsAffected: 3,
				},
			},
			execSQLStmts: s.execSQLStmtsAndCommit,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(10001)),
					int64(s.getUserID(10002)),
					int64(s.getUserID(10003)),
				})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(10001),
						OrderId: 10001,
						Content: "updated content",
						Account: 10001.0,
					},
					{
						UserId:  s.getUserID(10002),
						OrderId: 10002,
						Content: "updated content",
						Account: 10002.0,
					},
					{
						UserId:  s.getUserID(10003),
						OrderId: 10003,
						Content: "updated content",
						Account: 10003.0,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		// 无占位符_更新多行_回滚事务
		{
			name: "无占位符_更新多行_回滚事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 11001, 'initial content11001', 11001.0)", s.getUserID(11001)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 11002, 'initial content11002', 11002.0)", s.getUserID(11002)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 11003, 'initial content11003', 11003.0)", s.getUserID(11003)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			infos: []sqlInfo{
				{
					query: fmt.Sprintf("UPDATE `order` SET `content` = 'updated content' WHERE `user_id` IN (%d, %d, %d);",
						s.getUserID(11001), s.getUserID(11002), s.getUserID(11003)),
					rowsAffected: 3,
				},
			},
			execSQLStmts: s.execSQLStmtsAndRollback,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(11001)),
					int64(s.getUserID(11002)),
					int64(s.getUserID(11003)),
				})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(11001),
						OrderId: 11001,
						Content: "initial content11001",
						Account: 11001.0,
					},
					{
						UserId:  s.getUserID(11002),
						OrderId: 11002,
						Content: "initial content11002",
						Account: 11002.0,
					},
					{
						UserId:  s.getUserID(11003),
						OrderId: 11003,
						Content: "initial content11003",
						Account: 11003.0,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		// 有占位符_更新多行_回滚事务
		{
			name: "有占位符_更新多行_回滚事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 12001, 'initial content12001', 12001.0)", s.getUserID(12001)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 12002, 'initial content12002', 12002.0)", s.getUserID(12002)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 12003, 'initial content12003', 12003.0)", s.getUserID(12003)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			infos: []sqlInfo{
				{
					query: "UPDATE `order` SET `content` = 'updated content' WHERE `user_id` IN (?,?,?);",
					args: []any{
						s.getUserID(12001), s.getUserID(12002), s.getUserID(12003),
					},
					rowsAffected: 3,
				},
			},
			execSQLStmts: s.execSQLStmtsAndRollback,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(12001)),
					int64(s.getUserID(12002)),
					int64(s.getUserID(12003)),
				})
				wantOrderList := []Order{
					{
						UserId:  s.getUserID(12001),
						OrderId: 12001,
						Content: "initial content12001",
						Account: 12001.0,
					},
					{
						UserId:  s.getUserID(12002),
						OrderId: 12002,
						Content: "initial content12002",
						Account: 12002.0,
					},
					{
						UserId:  s.getUserID(12003),
						OrderId: 12003,
						Content: "initial content12003",
						Account: 12003.0,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},

		// 无占位符_删除多行_提交事务
		{
			name: "无占位符_删除多行_提交事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 13001, 'initial content13001', 13001.0)", s.getUserID(13001)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 13002, 'initial content13002', 13002.0)", s.getUserID(13002)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 13003, 'initial content13003', 13003.0)", s.getUserID(13003)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			infos: []sqlInfo{
				{
					query: fmt.Sprintf("DELETE FROM `order` WHERE `user_id` IN (%d, %d, %d)",
						s.getUserID(13001), s.getUserID(13002), s.getUserID(13003)),
					rowsAffected: 3,
				},
			},
			execSQLStmts: s.execSQLStmtsAndCommit,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(13001)),
					int64(s.getUserID(13002)),
					int64(s.getUserID(13003)),
				})
				orders := getOrdersFromRows(t, rows)
				assert.Equal(t, 0, len(orders))
			},
		},
		// 有占位符_删除多行_提交事务
		{
			name: "有占位符_删除多行_提交事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 14001, 'initial content14001', 14001.0)", s.getUserID(14001)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 14002, 'initial content14002', 14002.0)", s.getUserID(14002)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 14003, 'initial content14003', 14003.0)", s.getUserID(14003)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			infos: []sqlInfo{
				{
					query: "DELETE FROM `order` WHERE `user_id` IN (?, ?, ?)",
					args: []any{
						s.getUserID(14001), s.getUserID(14002), s.getUserID(14003),
					},
					rowsAffected: 3,
				},
			},
			execSQLStmts: s.execSQLStmtsAndCommit,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(14001)),
					int64(s.getUserID(14002)),
					int64(s.getUserID(14003)),
				})
				orders := getOrdersFromRows(t, rows)
				assert.Equal(t, 0, len(orders))
			},
		},
		// 无占位符_删除多行_回滚事务
		{
			name: "无占位符_删除多行_回滚事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 15001, 'initial content15001', 15001.0)", s.getUserID(15001)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 15002, 'initial content15002', 15002.0)", s.getUserID(15002)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 15003, 'initial content15003', 15003.0)", s.getUserID(15003)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			infos: []sqlInfo{
				{
					query: fmt.Sprintf("DELETE FROM `order` WHERE `user_id` IN (%d, %d, %d)",
						s.getUserID(15001), s.getUserID(15002), s.getUserID(15003)),
					rowsAffected: 3,
				},
			},
			execSQLStmts: s.execSQLStmtsAndRollback,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(15001)),
					int64(s.getUserID(15002)),
					int64(s.getUserID(15003)),
				})

				wantOrderList := []Order{
					{
						UserId:  s.getUserID(15001),
						OrderId: 15001,
						Content: "initial content15001",
						Account: 15001.0,
					},
					{
						UserId:  s.getUserID(15002),
						OrderId: 15002,
						Content: "initial content15002",
						Account: 15002.0,
					},
					{
						UserId:  s.getUserID(15003),
						OrderId: 15003,
						Content: "initial content15003",
						Account: 15003.0,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		// 有占位符_删除多行_回滚事务
		{
			name: "有占位符_删除多行_回滚事务",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 16001, 'initial content16001', 16001.0)", s.getUserID(16001)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 16002, 'initial content16002', 16002.0)", s.getUserID(16002)),
					fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (%d, 16003, 'initial content16003', 16003.0)", s.getUserID(16003)),
				}
				execSQL(t, s.db, sqls)
			},
			driverUsedOnlyCtxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			infos: []sqlInfo{
				{
					query: "DELETE FROM `order` WHERE `user_id` IN (?, ?, ?)",
					args: []any{
						s.getUserID(16001), s.getUserID(16002), s.getUserID(16003),
					},
					rowsAffected: 3,
				},
			},
			execSQLStmts: s.execSQLStmtsAndRollback,
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{
					int64(s.getUserID(16001)),
					int64(s.getUserID(16002)),
					int64(s.getUserID(16003)),
				})

				wantOrderList := []Order{
					{
						UserId:  s.getUserID(16001),
						OrderId: 16001,
						Content: "initial content16001",
						Account: 16001.0,
					},
					{
						UserId:  s.getUserID(16002),
						OrderId: 16002,
						Content: "initial content16002",
						Account: 16002.0,
					},
					{
						UserId:  s.getUserID(16003),
						OrderId: 16003,
						Content: "initial content16003",
						Account: 16003.0,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},

		// 无占位符_组合情况_提交事务
		// 有占位符_组合情况_提交事务
		// 无占位符_组合情况_回滚事务
		// 有占位符_组合情况_回滚事务
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// 准备测试数据
			tc.before(t)

			// 开启事务
			tx, err := s.db.BeginTx(tc.driverUsedOnlyCtxFunc(), nil)
			require.NoError(t, err)

			// 在事务tx中执行SQL语句
			tc.execSQLStmts(t, tc.infos, tx)

			// 验证结果, 使用s.db验证执行tc.sqlStmt后的影响
			tc.after(t)

			// 因并发测试的存在,所以不会清理表中所有数据
		})
	}
}

func (s *PrepareSingleTXTestSuite) getUserID(uid int) int {
	return uid + s.forwardPluginUsedOnlyClientID
}

func (s *PrepareSingleTXTestSuite) execSQLStmtsAndCommit(t *testing.T, infos []sqlInfo, tx *sql.Tx) {
	t.Helper()
	s.execSQLStmtsAndCommitOrRollback(t, infos, tx, func(tx *sql.Tx) error {
		return tx.Commit()
	})
}

func (s *PrepareSingleTXTestSuite) execSQLStmtsAndCommitOrRollback(t *testing.T, infos []sqlInfo, tx *sql.Tx, fn func(tx *sql.Tx) error) {
	t.Helper()

	stmts := make([]*sql.Stmt, 0, len(infos))

	for _, info := range infos {
		stmt, err := tx.Prepare(info.query)
		require.NoError(t, err)

		stmts = append(stmts, stmt)

		res, err := stmt.Exec(info.args...)
		require.NoError(t, err)

		affected, err := res.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, info.rowsAffected, affected)

		lastInsertId, err := res.LastInsertId()
		assert.NoError(t, err)
		assert.Equal(t, info.lastInsertId, lastInsertId)
	}
	// commit or rollback
	assert.NoError(t, fn(tx))

	for _, stmt := range stmts {
		assert.NoError(t, stmt.Close())
	}
}

func (s *PrepareSingleTXTestSuite) execSQLStmtsAndRollback(t *testing.T, infos []sqlInfo, tx *sql.Tx) {
	t.Helper()
	s.execSQLStmtsAndCommitOrRollback(t, infos, tx, func(tx *sql.Tx) error {
		return tx.Rollback()
	})
}
