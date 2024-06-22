//go:build e2e

package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"strconv"
	"strings"

	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/cluster"
	"github.com/meoying/dbproxy/internal/datasource/masterslave"
	"github.com/meoying/dbproxy/internal/datasource/shardingsource"
	logdriver "github.com/meoying/dbproxy/internal/driver/mysql/log"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	"github.com/stretchr/testify/assert"

	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/sharding"
	"github.com/meoying/dbproxy/internal/sharding/hash"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type TestShardingPluginSuite struct {
	suite.Suite
	server *Server
	db     *sql.DB
}

type Order struct {
	UserId  int
	OrderId int64
	Content string
	Account float64
}

func (s *TestShardingPluginSuite) createDatabase(db *sql.DB, dbName string) error {
	createDBSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName)
	_, err := db.Exec(createDBSQL)
	if err != nil {
		return fmt.Errorf("error creating database: %v", err)
	}
	return nil
}

func (s *TestShardingPluginSuite) createTable(db *sql.DB, name string) error {
	createTableSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		user_id INT NOT NULL,
		order_id BIGINT NOT NULL,
		content TEXT,
		account DOUBLE,
		PRIMARY KEY (user_id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`, name)

	// Execute the SQL statement
	_, err := db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}
	return nil
}

func (s *TestShardingPluginSuite) SetupSuite() {
	dsn := "root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := openDB(dsn)
	require.NoError(s.T(), err)
	// 创建db
	err = s.createDatabase(db, "order_db_0")
	require.NoError(s.T(), err)
	err = s.createDatabase(db, "order_db_1")
	require.NoError(s.T(), err)
	err = s.createDatabase(db, "order_db_2")
	require.NoError(s.T(), err)
	dsn0 := "root:root@tcp(127.0.0.1:13306)/order_db_0?charset=utf8mb4&parseTime=True&loc=Local"
	dsn1 := "root:root@tcp(127.0.0.1:13306)/order_db_1?charset=utf8mb4&parseTime=True&loc=Local"
	dsn2 := "root:root@tcp(127.0.0.1:13306)/order_db_2?charset=utf8mb4&parseTime=True&loc=Local"
	// 创建表
	db1, err := openDB(dsn0)
	require.NoError(s.T(), err)
	db2, err := openDB(dsn1)
	require.NoError(s.T(), err)
	db3, err := openDB(dsn2)
	require.NoError(s.T(), err)
	err = s.createTable(db1, "order_tab")
	require.NoError(s.T(), err)
	err = s.createTable(db2, "order_tab")
	require.NoError(s.T(), err)
	err = s.createTable(db3, "order_tab")
	require.NoError(s.T(), err)
	// 初始化
	dbBase := 3
	dbPattern, tablePattern, dsPattern := "order_db_%d", "order_tab", "0.db.cluster.company.com:3306"
	shardAlgorithm := &hash.Hash{
		ShardingKey:  "user_id",
		DBPattern:    &hash.Pattern{Name: dbPattern, Base: dbBase},
		TablePattern: &hash.Pattern{Name: tablePattern, NotSharding: true},
		DsPattern:    &hash.Pattern{Name: dsPattern, NotSharding: true},
	}
	m := map[string]*masterslave.MasterSlavesDB{
		"order_db_0": s.MasterSlavesMysqlDB(db1),
		"order_db_1": s.MasterSlavesMysqlDB(db2),
		"order_db_2": s.MasterSlavesMysqlDB(db3),
	}
	clusterDB := cluster.NewClusterDB(m)
	ds := map[string]datasource.DataSource{
		"0.db.cluster.company.com:3306": clusterDB,
	}
	dss := shardingsource.NewShardingDataSource(ds)
	p := sharding.NewPlugin(dss, shardAlgorithm)
	plugins := []plugin.Plugin{
		p,
	}
	server := NewServer(":8307", plugins)
	s.db = db
	s.server = server
	go func() {
		err := server.Start()
		s.T().Log(err)
	}()
}

func (s *TestShardingPluginSuite) TestSharding_Insert() {
	testcases := []struct {
		name  string
		sql   string
		after func(t *testing.T)
	}{
		{
			name: "插入多条数据",
			sql:  "INSERT INTO order (`user_id`,`order_id`,`content`,`account`) values (1,3,'content',1.1),(2,4,'content4',1.3),(3,3,'content3',1.3);",
			after: func(t *testing.T) {
				rowList := s.getRowsFromTable([]int64{1, 2, 3})
				// 表示每个库的数据
				wantOrderList := [][]Order{
					{
						{
							UserId:  3,
							OrderId: 3,
							Content: "content3",
							Account: 1.3,
						},
					},
					{
						{
							UserId:  1,
							OrderId: 3,
							Content: "content",
							Account: 1.1,
						},
					},
					{
						{
							UserId:  2,
							OrderId: 4,
							Content: "content4",
							Account: 1.3,
						},
					},
				}
				for idx, row := range rowList {
					orders := s.getColsFromRows(row)
					wantOrders := wantOrderList[idx]
					assert.ElementsMatch(t, wantOrders, orders)
				}
			},
		},
	}
	for _, tc := range testcases {
		s.T().Run(tc.name, func(t *testing.T) {
			db, err := openDB("root:root@tcp(localhost:8307)/")
			require.NoError(s.T(), err)
			_, err = db.Exec(tc.sql)
			tc.after(t)
			// 清理数据
			s.clearTable()
		})
	}

}

func (s *TestShardingPluginSuite) TestSharding_NormalSelect() {
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
				sql1 := "INSERT INTO order_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (2,4,'content4',1.3);"
				sql2 := "INSERT INTO order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (1,1,'content1',1.1);"
				_, err := s.db.Exec(sql1)
				require.NoError(s.T(), err)
				_, err = s.db.Exec(sql2)
				require.NoError(s.T(), err)
			},
			sql: "SELECT /* useMaster */ `user_id`,`order_id`,`content`,`account`   FROM order WHERE (user_id = 1) OR (user_id =2);",
			after: func(t *testing.T, rows *sql.Rows) {
				res := s.getColsFromRows(rows)
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
				sql1 := "insert into order_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) values (2,4,'content4',0.1);"
				sql2 := "insert into order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) values (1,1,'content1',6.9);"
				sql3 := "insert into order_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) values (3,1,'content1',7.1);"
				sql4 := "insert into order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) values (4,1,'content1',9.9);"
				s.execSql([]string{sql1, sql2, sql3, sql4})
			},
			sql: "SELECT /* useMaster */ AVG(`account`)  FROM order;",
			after: func(t *testing.T, rows *sql.Rows) {
				avgAccounts := make([]sql.NullFloat64, 0, 2)
				for rows.Next() {
					var avgAccount sql.NullFloat64
					err := rows.Scan(&avgAccount)
					require.NoError(s.T(), err)
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
				sql1 := "insert into order_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) values (2,4,'content4',0.1);"
				sql2 := "insert into order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) values (1,1,'content1',6.9);"
				sql3 := "insert into order_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) values (3,1,'content1',7.1);"
				sql4 := "insert into order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) values (4,1,'content1',9.9);"
				s.execSql([]string{sql1, sql2, sql3, sql4})
			},
			sql: "SELECT /* useMaster */ MAX(`account`)  FROM order;",
			after: func(t *testing.T, rows *sql.Rows) {
				maxAccounts := make([]sql.NullFloat64, 0, 2)
				for rows.Next() {
					var maxAccount sql.NullFloat64
					err := rows.Scan(&maxAccount)
					require.NoError(s.T(), err)
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
			name: "order by",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (1,8,'content4',1.2);",
					"INSERT INTO order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (4,9,'content4',1.4);",
					"INSERT INTO order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (7,9,'content4',1.1);",
					"INSERT INTO order_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (3,11,'content4',1.6);",
					"INSERT INTO order_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (6,8,'content4',1.1);",
					"INSERT INTO order_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (2,10,'content4',1.1);",
				}
				s.execSql(sqls)
			},
			sql: "SELECT /* useMaster */ `user_id`,`order_id`,`content`,`account`  FROM `order` ORDER BY `account` DESC,`order_id`;",
			after: func(t *testing.T, rows *sql.Rows) {
				res := s.getColsFromRows(rows)
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
			name: "group by",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (1,8,'content4',1.2);",
					"INSERT INTO order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (4,8,'content4',1.2);",
					"INSERT INTO order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (7,7,'content4',1.2);",
					"INSERT INTO order_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (3,8,'content4',1.2);",
					"INSERT INTO order_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (6,6,'content4',1.2);",
					"INSERT INTO order_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (9,7,'content4',1.2);",
					"INSERT INTO order_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (2,8,'content4',1.2);",
					"INSERT INTO order_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (5,6,'content4',1.2);",
					"INSERT INTO order_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (8,7,'content4',1.2);",
				}
				s.execSql(sqls)
			},
			sql: "SELECT /* useMaster */ `order_id` AS `oid`  FROM `order` GROUP BY `oid`;",
			after: func(t *testing.T, rows *sql.Rows) {
				oidGroups := make([]int64, 0, 3)
				for rows.Next() {
					var oidGroup int64
					err := rows.Scan(&oidGroup)
					require.NoError(s.T(), err)
					oidGroups = append(oidGroups, oidGroup)
				}
				assert.ElementsMatch(t, []int64{
					6, 7, 8,
				}, oidGroups)
			},
		},
		{
			name: "limit",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (1,8,'content4',1.2);",
					"INSERT INTO order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (4,8,'content4',1.2);",
					"INSERT INTO order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (7,7,'content4',1.2);",
					"INSERT INTO order_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (3,8,'content4',1.2);",
					"INSERT INTO order_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (6,6,'content4',1.2);",
					"INSERT INTO order_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (9,7,'content4',1.2);",
					"INSERT INTO order_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (2,8,'content4',1.2);",
					"INSERT INTO order_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (5,6,'content4',1.2);",
					"INSERT INTO order_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (8,7,'content4',1.2);",
				}
				s.execSql(sqls)
			},
			sql: "SELECT /* useMaster */ `user_id` AS `uid`  FROM `order` ORDER BY `uid` LIMIT 6 OFFSET 0;",
			after: func(t *testing.T, rows *sql.Rows) {
				oidGroups := make([]int64, 0, 3)
				for rows.Next() {
					var oidGroup int64
					err := rows.Scan(&oidGroup)
					require.NoError(s.T(), err)
					oidGroups = append(oidGroups, oidGroup)
				}
				assert.ElementsMatch(t, []int64{
					1, 2, 3, 4, 5, 6,
				}, oidGroups)
			},
		},
		{
			name: "select distinct",
			before: func(t *testing.T) {
				sqls := []string{
					"INSERT INTO order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (1,8,'content4',1.2);",
					"INSERT INTO order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (4,8,'content4',1.2);",
					"INSERT INTO order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (7,7,'content4',1.2);",
					"INSERT INTO order_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (3,8,'content4',1.2);",
					"INSERT INTO order_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (6,6,'content4',1.2);",
					"INSERT INTO order_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (9,7,'content4',1.2);",
					"INSERT INTO order_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (2,8,'content4',1.2);",
					"INSERT INTO order_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (5,6,'content4',1.2);",
					"INSERT INTO order_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (8,7,'content4',1.2);",
				}
				s.execSql(sqls)
			},
			sql: "SELECT /* useMaster */ DISTINCT order_id AS oid  FROM `order`;",
			after: func(t *testing.T, rows *sql.Rows) {
				oidGroups := make([]int64, 0, 3)
				for rows.Next() {
					var oidGroup int64
					err := rows.Scan(&oidGroup)
					require.NoError(s.T(), err)
					oidGroups = append(oidGroups, oidGroup)
				}
				assert.ElementsMatch(t, []int64{
					6, 7, 8,
				}, oidGroups)
			},
		},
	}
	for _, tc := range testcases {
		s.T().Run(tc.name, func(t *testing.T) {
			tc.before(t)
			db, err := openDB("root:root@tcp(localhost:8307)/mysql")
			require.NoError(t, err)
			// 使用主库查找
			rows, err := db.QueryContext(context.Background(), tc.sql)
			require.NoError(s.T(), err)
			tc.after(t, rows)
			fmt.Println(tc.name)
			// 清理数据
			s.clearTable()
		})
	}
}

func (s *TestShardingPluginSuite) clearTable() {
	for i := 0; i < 3; i++ {
		sql := fmt.Sprintf("delete from  order_db_%d.order_tab;", i)
		_, err := s.db.Exec(sql)
		require.NoError(s.T(), err)
	}
}

func (s *TestShardingPluginSuite) getRowsFromTable(ids []int64) []*sql.Rows {
	idStr := make([]string, 0, len(ids))
	for _, id := range ids {
		idStr = append(idStr, strconv.FormatInt(id, 10))
	}
	rowsList := make([]*sql.Rows, 0, 3)
	for i := 0; i < 3; i++ {
		query := fmt.Sprintf("select * from order_db_%d.order_tab where `user_id` in (%s)", i, strings.Join(idStr, ","))
		rows, err := s.db.Query(query)
		require.NoError(s.T(), err)
		rowsList = append(rowsList, rows)
	}
	return rowsList

}

func (s *TestShardingPluginSuite) execSql(sqls []string) {
	for _, vsql := range sqls {
		_, err := s.db.Exec(vsql)
		require.NoError(s.T(), err)
	}
}

func (s *TestShardingPluginSuite) getOrder(row *sql.Rows) (Order, error) {
	var order Order
	if row.Next() {
		err := row.Scan(&order.UserId, &order.OrderId, &order.Content, &order.Account)
		if err != nil {
			return Order{}, err
		}
	}
	return order, nil
}

func (s *TestShardingPluginSuite) getColsFromRows(rows *sql.Rows) []Order {
	res := make([]Order, 0, 2)
	for rows.Next() {
		order := Order{}
		err := rows.Scan(&order.UserId, &order.OrderId, &order.Content, &order.Account)
		require.NoError(s.T(), err)
		res = append(res, order)
	}
	return res
}

func (s *TestShardingPluginSuite) MasterSlavesMysqlDB(db *sql.DB) *masterslave.MasterSlavesDB {
	masterSlaveDB := masterslave.NewMasterSlavesDB(db)
	return masterSlaveDB
}

func TestTestShardingPluginSuite(t *testing.T) {
	suite.Run(t, new(TestShardingPluginSuite))
}

func openDB(dsn string) (*sql.DB, error) {
	return logdriver.Open(dsn)
}
