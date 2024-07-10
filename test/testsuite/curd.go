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

// CRUDTestSuite 属于基础测试集包含最简单增删改查语句、单机事务
// 主要用于验证客户端与dbproxy之间、dbproxy与MySQL之间的协议传输是否正确
type CRUDTestSuite struct {
	suite.Suite
	db *sql.DB
}

func (s *CRUDTestSuite) SetDB(db *sql.DB) {
	s.db = db
}

// TestSelect 测试查询语句
func (s *CRUDTestSuite) TestSelect() {
	t := s.T()
	// 初始化数据
	testcases := []struct {
		name string
		// 初始化数据
		before func(t *testing.T)
		// 处理
		after func(t *testing.T, rows *sql.Rows)
		// 执行的sql
		sql string
	}{
		{
			name: "简单查询",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (1,1,'content1',1.1), (2,4,'content4',1.3);",
				}
				execSQL(t, s.db, sqls)
			},
			sql: "SELECT /* useMaster */ `user_id`,`order_id`,`content`,`account` FROM `order` WHERE (`user_id` = 1) OR (`user_id` = 2);",
			after: func(t *testing.T, rows *sql.Rows) {
				res := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, []Order{
					{
						UserId:  2,
						OrderId: 4,
						Content: "content4",
						Account: 1.3,
					},
					{
						UserId:  1,
						OrderId: 1,
						Content: "content1",
						Account: 1.1,
					},
				}, res)
			},
		},
		{
			name: "聚合函数AVG",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) values (1,1,'content1',6.9),(2,4,'content4',0.1),(3,1,'content1',7.1),(4,1,'content1',9.9);",
				}
				execSQL(t, s.db, sqls)
			},
			sql: "SELECT /* useMaster */ AVG(`account`)  FROM `order`;",
			after: func(t *testing.T, rows *sql.Rows) {
				avgAccounts := make([]sql.NullFloat64, 0, 2)
				for rows.Next() {
					var avgAccount sql.NullFloat64
					err := rows.Scan(&avgAccount)
					require.NoError(t, err)
					avgAccounts = append(avgAccounts, avgAccount)
				}
				assert.ElementsMatch(t, []sql.NullFloat64{
					{
						Float64: 6,
						Valid:   true,
					},
				}, avgAccounts)
			},
		},
		{
			name: "聚合函数MAX",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) values (1,1,'content1',6.9),(2,4,'content4',0.1),(3,1,'content1',7.1),(4,1,'content1',9.9);",
				}
				execSQL(t, s.db, sqls)
			},
			sql: "SELECT /* useMaster */ MAX(`account`)  FROM `order`;",
			after: func(t *testing.T, rows *sql.Rows) {
				maxAccounts := make([]sql.NullFloat64, 0, 2)
				for rows.Next() {
					var maxAccount sql.NullFloat64
					err := rows.Scan(&maxAccount)
					require.NoError(t, err)
					maxAccounts = append(maxAccounts, maxAccount)
				}
				assert.ElementsMatch(t, []sql.NullFloat64{
					{
						Float64: 9.9,
						Valid:   true,
					},
				}, maxAccounts)
			},
		},
		{
			name: "ORDER BY",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (1,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (4,9,'content4',1.4);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (7,9,'content4',1.1);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (3,11,'content4',1.6);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (6,8,'content4',1.1);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (2,10,'content4',1.1);",
				}
				execSQL(t, s.db, sqls)
			},
			sql: "SELECT /* useMaster */ `user_id`,`order_id`,`content`,`account`  FROM `order` ORDER BY `account` DESC,`order_id`;",
			after: func(t *testing.T, rows *sql.Rows) {
				res := getOrdersFromRows(t, rows)
				assert.Equal(t, []Order{
					{
						UserId:  3,
						OrderId: 11,
						Content: "content4",
						Account: 1.6,
					},
					{
						UserId:  4,
						OrderId: 9,
						Content: "content4",
						Account: 1.4,
					},
					{
						UserId:  1,
						OrderId: 8,
						Content: "content4",
						Account: 1.2,
					},
					{
						UserId:  6,
						OrderId: 8,
						Content: "content4",
						Account: 1.1,
					},
					{
						UserId:  7,
						OrderId: 9,
						Content: "content4",
						Account: 1.1,
					},
					{
						UserId:  2,
						OrderId: 10,
						Content: "content4",
						Account: 1.1,
					},
				}, res)
			},
		},
		{
			name: "GROUP BY",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (1,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (4,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (7,7,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (3,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (6,6,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (9,7,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (2,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (5,6,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (8,7,'content4',1.2);",
				}
				execSQL(t, s.db, sqls)
			},
			sql: "SELECT /* useMaster */ `order_id` AS `oid`  FROM `order` GROUP BY `oid`;",
			after: func(t *testing.T, rows *sql.Rows) {
				oidGroups := make([]int64, 0, 3)
				for rows.Next() {
					var oidGroup int64
					err := rows.Scan(&oidGroup)
					require.NoError(t, err)
					oidGroups = append(oidGroups, oidGroup)
				}
				assert.ElementsMatch(t, []int64{
					6, 7, 8,
				}, oidGroups)
			},
		},
		{
			name: "Limit",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (1,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (4,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (7,7,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (3,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (6,6,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (9,7,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (2,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (5,6,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (8,7,'content4',1.2);",
				}
				execSQL(t, s.db, sqls)
			},
			sql: "SELECT /* useMaster */ `user_id` AS `uid`  FROM `order` ORDER BY `uid` LIMIT 6 OFFSET 0;",
			after: func(t *testing.T, rows *sql.Rows) {
				oidGroups := make([]int64, 0, 3)
				for rows.Next() {
					var oidGroup int64
					err := rows.Scan(&oidGroup)
					require.NoError(t, err)
					oidGroups = append(oidGroups, oidGroup)
				}
				assert.ElementsMatch(t, []int64{
					1, 2, 3, 4, 5, 6,
				}, oidGroups)
			},
		},
		{
			name: "SELECT DISTINCT",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (1,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (4,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (7,7,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (3,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (6,6,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (9,7,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (2,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (5,6,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (8,7,'content4',1.2);",
				}
				execSQL(t, s.db, sqls)
			},
			sql: "SELECT /* useMaster */ DISTINCT `order_id` AS `oid`  FROM `order`;",
			after: func(t *testing.T, rows *sql.Rows) {
				oidGroups := make([]int64, 0, 3)
				for rows.Next() {
					var oidGroup int64
					err := rows.Scan(&oidGroup)
					require.NoError(t, err)
					oidGroups = append(oidGroups, oidGroup)
				}
				assert.ElementsMatch(t, []int64{
					6, 7, 8,
				}, oidGroups)
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)
			// 使用主库查找
			rows, err := s.db.Query(tc.sql)
			require.NoError(t, err)
			tc.after(t, rows)
			// 清理数据
			clearTable(t, s.db)
		})
	}
}

// TestInsert 测试插入语句
func (s *CRUDTestSuite) TestInsert() {
	t := s.T()
	testcases := []struct {
		name   string
		sql    string
		before func(t *testing.T)
		after  func(t *testing.T)
	}{
		{
			name:   "插入多条数据",
			sql:    "INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) values (1,3,'content',1.1),(2,4,'content4',1.3),(3,3,'content3',1.3);",
			before: func(t *testing.T) {},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{1, 2, 3})
				// 表示每个库的数据
				wantOrderList := []Order{
					{
						UserId:  3,
						OrderId: 3,
						Content: "content3",
						Account: 1.3,
					},
					{
						UserId:  1,
						OrderId: 3,
						Content: "content",
						Account: 1.1,
					},
					{
						UserId:  2,
						OrderId: 4,
						Content: "content4",
						Account: 1.3,
					},
				}
				actualOrderList := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, actualOrderList)
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)

			_, err := s.db.Exec(tc.sql)
			require.NoError(t, err)

			tc.after(t)
			// 清理数据
			clearTable(t, s.db)
		})
	}
}

// TestUpdate 测试更新语句
func (s *CRUDTestSuite) TestUpdate() {
	t := s.T()
	testcases := []struct {
		name   string
		sql    string
		before func(t *testing.T)
		after  func(t *testing.T)
	}{
		{
			name: "更新一行",
			sql:  "UPDATE `order` SET `order_id` = 3,`content`='content',`account`=1.6 WHERE `user_id` = 1;",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (2,4,'content4',1.3);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (1,1,'content1',1.1);",
				}
				execSQL(t, s.db, sqls)
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{1, 2})
				wantOrderList := []Order{
					{
						UserId:  1,
						OrderId: 3,
						Content: "content",
						Account: 1.6,
					},
					{
						UserId:  2,
						OrderId: 4,
						Content: "content4",
						Account: 1.3,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		{
			name: "更新多行",
			sql:  "UPDATE `order` SET `order_id` = 3,`content`='content',`account`=1.6 WHERE `user_id` in (1,2);",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (2,4,'content4',1.3);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (1,1,'content1',1.1);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (3,1,'content1',1.1);",
				}
				execSQL(t, s.db, sqls)
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{1, 2, 3})
				wantOrderList := []Order{
					{
						UserId:  3,
						OrderId: 1,
						Content: "content1",
						Account: 1.1,
					},
					{
						UserId:  1,
						OrderId: 3,
						Content: "content",
						Account: 1.6,
					},
					{
						UserId:  2,
						OrderId: 3,
						Content: "content",
						Account: 1.6,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)

			_, err := s.db.Exec(tc.sql)
			require.NoError(t, err)

			tc.after(t)
			// 清理数据
			clearTable(t, s.db)
		})
	}
}

// TestDelete 测试删除语句
func (s *CRUDTestSuite) TestDelete() {
	t := s.T()
	testcases := []struct {
		name   string
		sql    string
		before func(t *testing.T)
		after  func(t *testing.T)
	}{
		{
			name: "删除一行",
			sql:  "DELETE FROM `order` WHERE `user_id` = 1;",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (2,4,'content4',1.3);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (1,1,'content1',1.1);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) VALUES (3,1,'content1',1.1);",
				}
				execSQL(t, s.db, sqls)
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{1, 2, 3})
				wantOrderList := []Order{
					{
						UserId:  3,
						OrderId: 1,
						Content: "content1",
						Account: 1.1,
					},
					{
						UserId:  2,
						OrderId: 4,
						Content: "content4",
						Account: 1.3,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)

			_, err := s.db.Exec(tc.sql)
			require.NoError(t, err)

			tc.after(t)
			// 清理数据
			clearTable(t, s.db)
		})
	}
}

// TestSingleTransaction 测试单机(节点)事务
func (s *CRUDTestSuite) TestSingleTransaction() {
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
			sqlStmts: []string{"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (1, 1001, 'sample content', 10.0);"},
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
				rows := getRowsFromTable(t, s.db, []int64{1})
				wantOrderList := []Order{
					{
						UserId:  1,
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
			sqlStmts: []string{"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (1, 1001, 'abc_sample content', 10.0)"},
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
			ctxFunc:  func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
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
			ctxFunc:  func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
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
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (2, 1002, 'initial content', 20.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc:  func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{"UPDATE `order` SET `content` = 'updated content' WHERE `user_id` = 2;"},
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
				rows := getRowsFromTable(t, s.db, []int64{2})
				wantOrderList := []Order{
					{
						UserId:  2,
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
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (2, 1002, 'initial content', 20.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (12, 1102, 'initial content', 120.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (112, 1112, 'initial content', 1120.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc:  func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
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
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (113, 1113, 'delete content', 1130.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc:  func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{"DELETE FROM `order` WHERE `user_id` = 113;"},
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
				rows := getRowsFromTable(t, s.db, []int64{113})
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
			ctxFunc:  func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
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
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
			sqlStmts: []string{
				"INSERT INTO `order` (user_id, order_id, content, account) VALUES (25, 2005, 'insert content', 250.0);",
				"UPDATE `order` SET `content` = 'updated content again' WHERE `user_id` = 22;",
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
				rows := getRowsFromTable(t, s.db, []int64{22, 25})
				wantOrderList := []Order{
					{
						UserId:  22,
						OrderId: 2002,
						Content: "updated content again",
						Account: 220.0,
					},
					{
						UserId:  25,
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
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (22, 2002, 'initial content', 220.0)",
					"INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (23, 2003, 'delete content', 230.0)",
				}
				execSQL(t, s.db, sqls)
			},
			ctxFunc: func() context.Context { return sharding.NewSingleTxContext(context.Background()) },
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
