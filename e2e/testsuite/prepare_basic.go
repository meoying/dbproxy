package testsuite

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type PrepareBasicTestSuite struct {
	suite.Suite
	db *sql.DB
}

func (s *PrepareBasicTestSuite) SetDB(db *sql.DB) {
	s.db = db
}

// TestPrepareSelect 测试 Prepare 查询语句
func (s *PrepareBasicTestSuite) TestPrepareSelect() {
	t := s.T()
	// 初始化数据
	testcases := []struct {
		name   string
		before func(t *testing.T)
		info   sqlInfo
		after  func(t *testing.T, rows *sql.Rows)
	}{
		{
			name: "无占位符_查询多行",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (21,21,'content21',21.21), (22,22,'content22',22.22);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query: "SELECT  /* @proxy useMaster=true */ `user_id`,`order_id`,`content`,`amount` FROM `order` WHERE (`user_id` = 21) OR (`user_id` = 22);",
			},
			after: func(t *testing.T, rows *sql.Rows) {
				res := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, []Order{
					{
						UserId:  21,
						OrderId: 21,
						Content: "content21",
						Amount:  21.21,
					},
					{
						UserId:  22,
						OrderId: 22,
						Content: "content22",
						Amount:  22.22,
					},
				}, res)
			},
		},
		{
			name: "有占位符_查询多行",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (21,1,'content1',1.1), (22,4,'content4',1.3);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query: "SELECT  /* @proxy useMaster=true */ `user_id`,`order_id`,`content`,`amount` FROM `order` WHERE (`user_id` = ?) OR (`user_id` = ?);",
				args:  []any{21, 22},
			},
			after: func(t *testing.T, rows *sql.Rows) {
				res := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, []Order{
					{
						UserId:  22,
						OrderId: 4,
						Content: "content4",
						Amount:  1.3,
					},
					{
						UserId:  21,
						OrderId: 1,
						Content: "content1",
						Amount:  1.1,
					},
				}, res)
			},
		},
		{
			name: "无占位符_聚合函数AVG",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) values (1,1,'content1',6.9),(2,4,'content4',0.1),(3,1,'content1',7.1),(4,1,'content1',9.9);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query: "SELECT  /* @proxy useMaster=true */ AVG(`amount`)  FROM `order`",
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
			name: "有占位符_聚合函数AVG",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) values (1,1,'content1',6.9),(2,4,'content4',0.1),(3,1,'content1',7.1),(4,1,'content1',9.9);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query: "SELECT  /* @proxy useMaster=true */ AVG(`amount`)  FROM `order` WHERE `user_id` IN (?, ?, ?, ?);",
				args:  []any{1, 2, 3, 4},
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
			name: "无占位符_聚合函数MAX",
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
			name: "有占位符_聚合函数MAX",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) values (1,1,'content1',6.9),(2,4,'content4',0.1),(3,1,'content1',7.1),(4,1,'content1',9.9);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query: "SELECT  /* @proxy useMaster=true */ MAX(`amount`)  FROM `order` WHERE `user_id` IN (?, ?, ?, ?);",
				args:  []any{1, 2, 3, 4},
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
			name: "无占位符_ORDER BY",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (1,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (2,10,'content4',1.1);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (3,11,'content4',1.6);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (4,9,'content4',1.4);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (6,8,'content4',1.1);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (7,9,'content4',1.1);",
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
			name: "有占位符_ORDER BY",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (1,8,'content4',1.2);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (2,10,'content4',1.1);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (3,11,'content4',1.6);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (4,9,'content4',1.4);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (6,8,'content4',1.1);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (7,9,'content4',1.1);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query: "SELECT  /* @proxy useMaster=true */ `user_id`,`order_id`,`content`,`amount`  FROM `order` WHERE `user_id` IN (?,?,?,?,?,?) ORDER BY `amount` DESC,`order_id`;",
				args:  []any{1, 2, 3, 4, 6, 7},
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
			name: "无占位符_GROUP BY",
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
			name: "有占位符_GROUP BY",
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
				query: "SELECT  /* @proxy useMaster=true */ `order_id` AS `oid`  FROM `order` WHERE `user_id` IN (?, ?, ?, ?, ?, ?, ?, ?, ?) GROUP BY `oid`;",
				args:  []any{1, 2, 3, 4, 5, 6, 7, 8, 9},
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
			name: "无占位符_Limit",
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
			name: "有占位符_Limit",
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
				query: "SELECT  /* @proxy useMaster=true */ `user_id` AS `uid`  FROM `order` WHERE `user_id` IN (?, ?, ?, ?, ?, ?, ?, ?, ?) ORDER BY `uid` LIMIT 6 OFFSET 0;",
				args:  []any{1, 2, 3, 4, 5, 6, 7, 8, 9},
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
			name: "无占位符_SELECT DISTINCT",
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
			name: "有占位符_SELECT DISTINCT",
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
				query: "SELECT  /* @proxy useMaster=true */ DISTINCT `order_id` AS `oid`  FROM `order` WHERE `user_id` IN (?, ?, ?, ?, ?, ?, ?, ?, ?);",
				args:  []any{1, 2, 3, 4, 5, 6, 7, 8, 9},
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
			name: "无占位符_WHERE子句中多个OR带括号连接",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (2,4,'content4',1.3);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (1,1,'content1',1.1);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (3,1,'content1',1.1);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query: "SELECT  /* @proxy useMaster=true */ `user_id`,`order_id`,`content`,`amount` FROM `order` WHERE (`user_id` = 3) OR (`user_id` = 1) OR (`user_id` = 2);",
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
		{
			name: "有占位符_WHERE子句中多个OR带括号连接",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (2,4,'content4',1.3);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (1,1,'content1',1.1);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (3,1,'content1',1.1);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query: "SELECT  /* @proxy useMaster=true */ `user_id`,`order_id`,`content`,`amount` FROM `order` WHERE (`user_id` = ?) OR (`user_id` = ?) OR (`user_id` = ?);",
				args:  []any{1, 2, 3},
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
			stmt, err := s.db.Prepare(tc.info.query)
			require.NoError(t, err)

			rows, err := stmt.Query(tc.info.args...)
			require.NoError(t, err)

			tc.after(t, rows)

			require.NoError(t, stmt.Close())

			// 清理数据
			ClearTables(t, s.db)
		})
	}
}

// TestPrepareInsert 测试 Prepare 插入语句
func (s *PrepareBasicTestSuite) TestPrepareInsert() {
	t := s.T()
	testcases := []struct {
		name   string
		before func(t *testing.T)
		info   sqlInfo
		after  func(t *testing.T)
	}{
		{
			name:   "无占位符_插入多行",
			before: func(t *testing.T) {},
			info: sqlInfo{
				query:        "INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) values (31, 31, 'content31', 31.31),(32, 32, 'content32', 32.32),(33, 33, 'content33', 33.33);",
				rowsAffected: 3,
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{31, 32, 33})
				// 表示每个库的数据
				wantOrderList := []Order{
					{
						UserId:  31,
						OrderId: 31,
						Content: "content31",
						Amount:  31.31,
					},
					{
						UserId:  32,
						OrderId: 32,
						Content: "content32",
						Amount:  32.32,
					},
					{
						UserId:  33,
						OrderId: 33,
						Content: "content33",
						Amount:  33.33,
					},
				}
				actualOrderList := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, actualOrderList)
			},
		},
		{
			name:   "有占位符_插入多行",
			before: func(t *testing.T) {},
			info: sqlInfo{
				query: "INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) values (?,?,?,?),(?,?,?,?),(?,?,?,?);",
				args: []any{
					34, 34, "content34", 34.34,
					35, 35, "content35", 35.35,
					36, 36, "content36", 36.36,
				},
				rowsAffected: 3,
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{34, 35, 36})
				// 表示每个库的数据
				wantOrderList := []Order{
					{
						UserId:  34,
						OrderId: 34,
						Content: "content34",
						Amount:  34.34,
					},
					{
						UserId:  35,
						OrderId: 35,
						Content: "content35",
						Amount:  35.35,
					},
					{
						UserId:  36,
						OrderId: 36,
						Content: "content36",
						Amount:  36.36,
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

			// 使用主库查找
			stmt, err := s.db.Prepare(tc.info.query)
			require.NoError(t, err)

			res, err := stmt.Exec(tc.info.args...)
			require.NoError(t, err)

			affected, err := res.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, tc.info.rowsAffected, affected)

			id, err := res.LastInsertId()
			assert.NoError(t, err)
			assert.Equal(t, tc.info.lastInsertId, id)

			tc.after(t)

			require.NoError(t, stmt.Close())

			// 清理数据
			ClearTables(t, s.db)
		})
	}
}

// TestPrepareUpdate 测试 Prepare 更新语句
func (s *PrepareBasicTestSuite) TestPrepareUpdate() {
	t := s.T()
	testcases := []struct {
		name   string
		before func(t *testing.T)
		info   sqlInfo
		after  func(t *testing.T)
	}{
		{
			name: "无占位符_更新多行",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (51,51,'content51',51.51);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (52,52,'content52',52.52);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (53,53,'content53',53.53);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query:        "UPDATE `order` SET `order_id` = 2,`content`='content2',`amount`=2.0 WHERE `user_id` = 51 OR `order_id` = 52;",
				rowsAffected: 2,
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{51, 52, 53})
				wantOrderList := []Order{
					{
						UserId:  51,
						OrderId: 2,
						Content: "content2",
						Amount:  2.0,
					},
					{
						UserId:  52,
						OrderId: 2,
						Content: "content2",
						Amount:  2.0,
					},
					{
						UserId:  53,
						OrderId: 53,
						Content: "content53",
						Amount:  53.53,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		{
			name: "有占位符_更新多行",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (51,51,'content51',51.51);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (52,52,'content52',52.52);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (53,53,'content53',53.53);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (54,54,'content54',54.54);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query:        "UPDATE `order` SET `order_id` = 2,`content`='content2',`amount`=2.0 WHERE `user_id` = ? OR `user_id` = ? OR `order_id` = ?;",
				args:         []any{52, 53, 54},
				rowsAffected: 3,
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{51, 52, 53, 54})
				wantOrderList := []Order{
					{
						UserId:  51,
						OrderId: 51,
						Content: "content51",
						Amount:  51.51,
					},
					{
						UserId:  52,
						OrderId: 2,
						Content: "content2",
						Amount:  2.0,
					},
					{
						UserId:  53,
						OrderId: 2,
						Content: "content2",
						Amount:  2.0,
					},
					{
						UserId:  54,
						OrderId: 2,
						Content: "content2",
						Amount:  2.0,
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

			// 使用主库查找
			stmt, err := s.db.Prepare(tc.info.query)
			require.NoError(t, err)

			res, err := stmt.Exec(tc.info.args...)
			require.NoError(t, err)

			affected, err := res.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, tc.info.rowsAffected, affected)

			id, err := res.LastInsertId()
			assert.NoError(t, err)
			assert.Equal(t, tc.info.lastInsertId, id)

			tc.after(t)

			require.NoError(t, stmt.Close())

			// 清理数据
			ClearTables(t, s.db)
		})
	}
}

// TestPrepareDelete 测试 Prepare 删除语句
func (s *PrepareBasicTestSuite) TestPrepareDelete() {
	t := s.T()
	testcases := []struct {
		name   string
		before func(t *testing.T)
		info   sqlInfo
		after  func(t *testing.T)
	}{
		{
			name: "无占位符_删除多行_分片列与非分片列混合",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (61,61,'content61',61.61);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (62,62,'content62',62.62);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (63,63,'content63',63.63);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (64,64,'content64',64.64);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (65,65,'content65',65.65);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query:        "DELETE FROM `order` WHERE `user_id` = 61 OR `user_id` = 64 OR `order_id` = 65;",
				rowsAffected: 3,
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{61, 62, 63, 64, 65})
				wantOrderList := []Order{
					{
						UserId:  62,
						OrderId: 62,
						Content: "content62",
						Amount:  62.62,
					},
					{
						UserId:  63,
						OrderId: 63,
						Content: "content63",
						Amount:  63.63,
					},
				}
				orders := getOrdersFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		{
			name: "有占位符_删除多行_分片列与非分片列混合",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (71,71,'content71',71.71);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (72,72,'content72',72.72);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (73,73,'content73',73.73);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (74,74,'content74',74.74);",
					"INSERT INTO `order` (`user_id`,`order_id`,`content`,`amount`) VALUES (75,75,'content75',75.75);",
				}
				execSQL(t, s.db, sqls)
			},
			info: sqlInfo{
				query:        "DELETE FROM `order` WHERE `user_id` = ? OR `user_id` = ? OR `order_id` = ? OR `content` = ?",
				args:         []any{71, 72, 73, "content74"},
				rowsAffected: 4,
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := getRowsFromTable(t, s.db, []int64{71, 72, 73, 74, 75})
				wantOrderList := []Order{
					{
						UserId:  75,
						OrderId: 75,
						Content: "content75",
						Amount:  75.75,
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

			// 使用主库查找
			stmt, err := s.db.Prepare(tc.info.query)
			require.NoError(t, err)

			res, err := stmt.Exec(tc.info.args...)
			require.NoError(t, err)

			affected, err := res.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, tc.info.rowsAffected, affected)

			id, err := res.LastInsertId()
			assert.NoError(t, err)
			assert.Equal(t, tc.info.lastInsertId, id)

			tc.after(t)

			require.NoError(t, stmt.Close())

			// 清理数据
			ClearTables(t, s.db)
		})
	}
}
