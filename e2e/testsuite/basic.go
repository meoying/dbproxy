package testsuite

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// BasicTestSuite 属于基础测试集包含最简单增删改查语句、单机事务
// 主要用于验证客户端与dbproxy之间、dbproxy与MySQL之间的协议传输是否正确
type BasicTestSuite struct {
	suite.Suite
	db *sql.DB
}

func (s *BasicTestSuite) SetDB(db *sql.DB) {
	s.db = db
}

// TestSelect 测试查询语句
func (s *BasicTestSuite) TestSelect() {
	t := s.T()
	testcases := []struct {
		name   string
		before func(t *testing.T)
		info   sqlInfo
		after  func(t *testing.T, rows *sql.Rows)
	}{
		{
			name: "简单查询",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (1,1,'content1',1.1), (2,4,'content4',1.3);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query: "SELECT  /* @proxy useMaster=true */ `user_id`,`order_id`,`content`,`amount` FROM `order` WHERE (`user_id` = 1) OR (`user_id` = 2);",
			},
			after: func(t *testing.T, rows *sql.Rows) {
				res := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, []Order{
					{
						UserId:  2,
						OrderId: 4,
						Content: "content4",
						Amount:  1.3,
					},
					{
						UserId:  1,
						OrderId: 1,
						Content: "content1",
						Amount:  1.1,
					},
				}, res)
			},
		},
		{
			name: "聚合函数AVG",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) values (1,1,'content1',6.9),(2,4,'content4',0.1),(3,1,'content1',7.1),(4,1,'content1',9.9);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query: "SELECT  /* @proxy useMaster=true */ AVG(`amount`)  FROM `order`;",
			},
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
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) values (1,1,'content1',6.9),(2,4,'content4',0.1),(3,1,'content1',7.1),(4,1,'content1',9.9);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query: "SELECT  /* @proxy useMaster=true */ MAX(`amount`)  FROM `order`;",
			},
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
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (1,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (4,9,'content4',1.4);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (7,9,'content4',1.1);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (3,11,'content4',1.6);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (6,8,'content4',1.1);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (2,10,'content4',1.1);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query: "SELECT  /* @proxy useMaster=true */ `user_id`,`order_id`,`content`,`amount`  FROM `order` ORDER BY `amount` DESC,`order_id`;",
			},
			after: func(t *testing.T, rows *sql.Rows) {
				res := getOrdersFromRows(t, rows)
				assert.Equal(t, []Order{
					{
						UserId:  3,
						OrderId: 11,
						Content: "content4",
						Amount:  1.6,
					},
					{
						UserId:  4,
						OrderId: 9,
						Content: "content4",
						Amount:  1.4,
					},
					{
						UserId:  1,
						OrderId: 8,
						Content: "content4",
						Amount:  1.2,
					},
					{
						UserId:  6,
						OrderId: 8,
						Content: "content4",
						Amount:  1.1,
					},
					{
						UserId:  7,
						OrderId: 9,
						Content: "content4",
						Amount:  1.1,
					},
					{
						UserId:  2,
						OrderId: 10,
						Content: "content4",
						Amount:  1.1,
					},
				}, res)
			},
		},
		{
			name: "GROUP BY",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (1,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (4,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (7,7,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (3,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (6,6,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (9,7,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (2,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (5,6,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (8,7,'content4',1.2);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query: "SELECT  /* @proxy useMaster=true */ `order_id` AS `oid`  FROM `order` GROUP BY `oid`;",
			},
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
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (1,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (4,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (7,7,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (3,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (6,6,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (9,7,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (2,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (5,6,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (8,7,'content4',1.2);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query: "SELECT  /* @proxy useMaster=true */ `user_id` AS `uid`  FROM `order` ORDER BY `uid` LIMIT 6 OFFSET 0;",
			},
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
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (1,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (4,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (7,7,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (3,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (6,6,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (9,7,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (2,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (5,6,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (8,7,'content4',1.2);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query: "SELECT  /* @proxy useMaster=true */ DISTINCT `order_id` AS `oid`  FROM `order`;",
			},
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
			name: "WHERE子句中多个OR带括号连接",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (2,4,'content4',1.3);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (1,1,'content1',1.1);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (3,1,'content1',1.1);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query: "SELECT  /* @proxy useMaster=true */ `user_id`,`order_id`,`content`,`amount` FROM `order` WHERE (`user_id` = 1) OR (`user_id` =2) OR (`user_id` = 3);",
			},
			after: func(t *testing.T, rows *sql.Rows) {
				res := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, []Order{
					{
						UserId:  2,
						OrderId: 4,
						Content: "content4",
						Amount:  1.3,
					},
					{
						UserId:  1,
						OrderId: 1,
						Content: "content1",
						Amount:  1.1,
					},
					{
						UserId:  3,
						OrderId: 1,
						Content: "content1",
						Amount:  1.1,
					},
				}, res)
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before(t)
			// 使用主库查找
			rows, err := s.db.Query(tc.info.query)
			require.NoError(t, err)
			tc.after(t, rows)
			// 清理数据
			ClearTables(t, s.db)
		})
	}
}

// TestInsert 测试插入语句
func (s *BasicTestSuite) TestInsert() {
	t := s.T()
	testcases := []struct {
		name   string
		before func(t *testing.T)
		info   sqlInfo
		after  func(t *testing.T)
	}{
		{
			name:   "插入多行",
			before: func(t *testing.T) {},
			info: sqlInfo{
				query:        "INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) values (1,3,'content',1.1),(2,4,'content4',1.3),(3,3,'content3',1.3);",
				rowsAffected: 3,
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{1, 2, 3})
				// 表示每个库的数据
				wantOrderList := []Order{
					{
						UserId:  3,
						OrderId: 3,
						Content: "content3",
						Amount:  1.3,
					},
					{
						UserId:  1,
						OrderId: 3,
						Content: "content",
						Amount:  1.1,
					},
					{
						UserId:  2,
						OrderId: 4,
						Content: "content4",
						Amount:  1.3,
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

			res, err := s.db.Exec(tc.info.query)
			require.NoError(t, err)

			affected, err := res.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, tc.info.rowsAffected, affected)

			id, err := res.LastInsertId()
			assert.NoError(t, err)
			assert.Equal(t, tc.info.lastInsertId, id)

			tc.after(t)
			// 清理数据
			ClearTables(t, s.db)
		})
	}
}

// TestUpdate 测试更新语句
func (s *BasicTestSuite) TestUpdate() {
	t := s.T()
	testcases := []struct {
		name   string
		before func(t *testing.T)
		info   sqlInfo
		after  func(t *testing.T)
	}{
		{
			name: "更新一行",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (2,4,'content4',1.3);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (1,1,'content1',1.1);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query:        "UPDATE `order` SET `order_id` = 3,`content`='content',`amount`=1.6 WHERE `user_id` = 1;",
				rowsAffected: 1,
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{1, 2})
				wantOrderList := []Order{
					{
						UserId:  1,
						OrderId: 3,
						Content: "content",
						Amount:  1.6,
					},
					{
						UserId:  2,
						OrderId: 4,
						Content: "content4",
						Amount:  1.3,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		{
			name: "更新多行",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (1,1,'content1',1.1);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (2,4,'content4',1.3);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (3,1,'content1',1.1);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query:        "UPDATE `order` SET `order_id` = 3,`content`='content',`amount`=1.6 WHERE `user_id` = 1 OR `order_id` = 4;",
				rowsAffected: 2,
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{1, 2, 3})
				wantOrderList := []Order{
					{
						UserId:  1,
						OrderId: 3,
						Content: "content",
						Amount:  1.6,
					},
					{
						UserId:  2,
						OrderId: 3,
						Content: "content",
						Amount:  1.6,
					},
					{
						UserId:  3,
						OrderId: 1,
						Content: "content1",
						Amount:  1.1,
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

			res, err := s.db.Exec(tc.info.query)
			require.NoError(t, err)

			affected, err := res.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, tc.info.rowsAffected, affected)

			id, err := res.LastInsertId()
			assert.NoError(t, err)
			assert.Equal(t, tc.info.lastInsertId, id)

			tc.after(t)
			// 清理数据
			ClearTables(t, s.db)
		})
	}
}

// TestDelete 测试删除语句
func (s *BasicTestSuite) TestDelete() {
	t := s.T()
	testcases := []struct {
		name   string
		before func(t *testing.T)
		info   sqlInfo
		after  func(t *testing.T)
	}{
		{
			name: "删除多行_分片列与非分片列混合",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (1,1,'content1',1.1);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (2,4,'content4',1.3);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (3,1,'content1',1.1);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (4,4,'content4',1.4);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (5,5,'content5',1.5);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query:        "DELETE FROM `order` WHERE `user_id` = 1 OR `user_id` = 4 OR `order_id` = 5;",
				rowsAffected: 3,
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{1, 2, 3, 4, 5})
				wantOrderList := []Order{
					{
						UserId:  3,
						OrderId: 1,
						Content: "content1",
						Amount:  1.1,
					},
					{
						UserId:  2,
						OrderId: 4,
						Content: "content4",
						Amount:  1.3,
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

			res, err := s.db.Exec(tc.info.query)
			require.NoError(t, err)

			affected, err := res.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, tc.info.rowsAffected, affected)

			id, err := res.LastInsertId()
			assert.NoError(t, err)
			assert.Equal(t, tc.info.lastInsertId, id)

			tc.after(t)
			// 清理数据
			ClearTables(t, s.db)
		})
	}
}
