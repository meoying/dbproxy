//go:build e2e

package sharding_test

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ecodeclub/ekit/retry"
	"github.com/go-sql-driver/mysql"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/cluster"
	"github.com/meoying/dbproxy/internal/datasource/masterslave"
	"github.com/meoying/dbproxy/internal/datasource/shardingsource"
	logdriver "github.com/meoying/dbproxy/internal/protocol/mysql/driver/log"
	"github.com/meoying/dbproxy/internal/protocol/mysql/driver/sharding"
	"github.com/meoying/dbproxy/internal/sharding/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestShardingDriverTestSuite(t *testing.T) {
	suite.Run(t, new(shardingDriverTestSuite))
}

type shardingDriverTestSuite struct {
	suite.Suite
	db *sql.DB
}

type Order struct {
	UserId  int
	OrderId int64
	Content string
	Account float64
}

func (s *shardingDriverTestSuite) createDatabases(db *sql.DB, names ...string) {
	for _, name := range names {
		_, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", name))
		s.NoError(err, fmt.Errorf("创建库=%s失败", name))
	}
}

func (s *shardingDriverTestSuite) createTables(db *sql.DB, names ...string) {
	const tableTemplate = "CREATE TABLE IF NOT EXISTS `%s` " +
		"(" +
		"user_id INT NOT NULL," +
		"order_id BIGINT NOT NULL," +
		"content TEXT," +
		"account DOUBLE," +
		"PRIMARY KEY (user_id)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"

	for _, name := range names {
		_, err := db.Exec(fmt.Sprintf(tableTemplate, name))
		s.NoError(err, fmt.Errorf("创建表=%s失败", name))
	}
}

func (s *shardingDriverTestSuite) SetupSuite() {

	db := WaitForMySQLSetup(s.newDSN(""))

	// 初始化
	dbBase := 3
	dbPattern, tablePattern, dsPattern := "driver_db_%d", "order_tab", "0.db.cluster.company.com:3306"

	shardAlgorithm := &hash.Hash{
		ShardingKey:  "user_id",
		DBPattern:    &hash.Pattern{Name: dbPattern, Base: dbBase},
		TablePattern: &hash.Pattern{Name: tablePattern, NotSharding: true},
		DsPattern:    &hash.Pattern{Name: dsPattern, NotSharding: true},
	}

	dbNames := make([]string, 0, dbBase)
	for i := 0; i < dbBase; i++ {
		dbNames = append(dbNames, fmt.Sprintf(dbPattern, i))
	}

	s.createDatabases(db, dbNames...)

	// dsn0 := "root:root@tcp(127.0.0.1:13306)/driver_db_0?charset=utf8mb4&parseTime=True&loc=Local"
	// dsn1 := "root:root@tcp(127.0.0.1:13306)/driver_db_1?charset=utf8mb4&parseTime=True&loc=Local"
	// dsn2 := "root:root@tcp(127.0.0.1:13306)/driver_db_2?charset=utf8mb4&parseTime=True&loc=Local"

	dbs := make([]*sql.DB, 0, len(dbNames))
	m := make(map[string]*masterslave.MasterSlavesDB, len(dbNames))
	for _, name := range dbNames {
		d, err := openDB(s.newDSN(name))
		s.NoError(err)
		s.createTables(d, tablePattern)
		dbs = append(dbs, d)
		m[name] = masterslave.NewMasterSlavesDB(d)
	}

	ds := map[string]datasource.DataSource{
		"0.db.cluster.company.com:3306": cluster.NewClusterDB(m),
	}
	sds := shardingsource.NewShardingDataSource(ds)

	connector, err := sharding.NewConnector(sds, shardAlgorithm)
	s.NoError(err)
	s.db = sql.OpenDB(connector)
}

func (s *shardingDriverTestSuite) newDSN(name string) string {
	return fmt.Sprintf("root:root@tcp(127.0.0.1:13306)/%s?charset=utf8mb4&parseTime=True&loc=Local", name)
}

func WaitForMySQLSetup(dsn string) *sql.DB {
	sqlDB, err := openDB(dsn)
	if err != nil {
		panic(err)
	}
	const maxInterval = 10 * time.Second
	const maxRetries = 10
	strategy, err := retry.NewExponentialBackoffRetryStrategy(time.Second, maxInterval, maxRetries)
	if err != nil {
		panic(err)
	}
	const timeout = 5 * time.Second
	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		err = sqlDB.PingContext(ctx)
		cancel()
		if err == nil {
			break
		}
		next, ok := strategy.Next()
		if !ok {
			panic("WaitForMySQLSetup 重试失败......")
		}
		time.Sleep(next)
	}
	return sqlDB
}

// openDB
// TODO: 暂时用mysql driver来创建数据库和表, 用sharding driver来执行,后续sharding driver要平替
func openDB(dsn string) (*sql.DB, error) {
	l := slog.New(slog.NewTextHandler(os.Stdout, nil))
	connector, err := logdriver.NewConnector(&mysql.MySQLDriver{}, dsn, logdriver.WithLogger(l))
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(connector), nil
}

// TODO: TearDownSuite

func (s *shardingDriverTestSuite) TestDriver_Select() {
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
					"INSERT INTO `driver_db_2.order_tab` (`user_id`,`order_id`,`content`,`account`) VALUES (2,4,'content4',1.3);",
					"INSERT INTO `driver_db_1.order_tab` (`user_id`,`order_id`,`content`,`account`) VALUES (1,1,'content1',1.1);",
				}
				s.execSql(t, sqls)
			},
			sql: "SELECT /* useMaster */ `user_id`,`order_id`,`content`,`account` FROM `order` WHERE (`user_id` = 1) OR (`user_id` = 2);",
			after: func(t *testing.T, rows *sql.Rows) {
				res := s.getColsFromRows(t, rows)
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
					"INSERT INTO driver_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) values (2,4,'content4',0.1);",
					"INSERT INTO driver_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) values (1,1,'content1',6.9);",
					"INSERT INTO driver_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) values (3,1,'content1',7.1);",
					"INSERT INTO driver_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) values (4,1,'content1',9.9);",
				}
				s.execSql(t, sqls)
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
					"INSERT INTO driver_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) values (2,4,'content4',0.1);",
					"INSERT INTO driver_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) values (1,1,'content1',6.9);",
					"INSERT INTO driver_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) values (3,1,'content1',7.1);",
					"INSERT INTO driver_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) values (4,1,'content1',9.9);",
				}
				s.execSql(t, sqls)
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
					"INSERT INTO driver_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (1,8,'content4',1.2);",
					"INSERT INTO driver_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (4,9,'content4',1.4);",
					"INSERT INTO driver_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (7,9,'content4',1.1);",
					"INSERT INTO driver_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (3,11,'content4',1.6);",
					"INSERT INTO driver_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (6,8,'content4',1.1);",
					"INSERT INTO driver_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (2,10,'content4',1.1);",
				}
				s.execSql(t, sqls)
			},
			sql: "SELECT /* useMaster */ `user_id`,`order_id`,`content`,`account`  FROM `order` ORDER BY `account` DESC,`order_id`;",
			after: func(t *testing.T, rows *sql.Rows) {
				res := s.getColsFromRows(t, rows)
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
					"INSERT INTO driver_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (1,8,'content4',1.2);",
					"INSERT INTO driver_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (4,8,'content4',1.2);",
					"INSERT INTO driver_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (7,7,'content4',1.2);",
					"INSERT INTO driver_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (3,8,'content4',1.2);",
					"INSERT INTO driver_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (6,6,'content4',1.2);",
					"INSERT INTO driver_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (9,7,'content4',1.2);",
					"INSERT INTO driver_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (2,8,'content4',1.2);",
					"INSERT INTO driver_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (5,6,'content4',1.2);",
					"INSERT INTO driver_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (8,7,'content4',1.2);",
				}
				s.execSql(t, sqls)
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
					"INSERT INTO driver_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (1,8,'content4',1.2);",
					"INSERT INTO driver_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (4,8,'content4',1.2);",
					"INSERT INTO driver_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (7,7,'content4',1.2);",
					"INSERT INTO driver_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (3,8,'content4',1.2);",
					"INSERT INTO driver_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (6,6,'content4',1.2);",
					"INSERT INTO driver_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (9,7,'content4',1.2);",
					"INSERT INTO driver_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (2,8,'content4',1.2);",
					"INSERT INTO driver_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (5,6,'content4',1.2);",
					"INSERT INTO driver_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (8,7,'content4',1.2);",
				}
				s.execSql(t, sqls)
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
					"INSERT INTO driver_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (1,8,'content4',1.2);",
					"INSERT INTO driver_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (4,8,'content4',1.2);",
					"INSERT INTO driver_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (7,7,'content4',1.2);",
					"INSERT INTO driver_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (3,8,'content4',1.2);",
					"INSERT INTO driver_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (6,6,'content4',1.2);",
					"INSERT INTO driver_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (9,7,'content4',1.2);",
					"INSERT INTO driver_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (2,8,'content4',1.2);",
					"INSERT INTO driver_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (5,6,'content4',1.2);",
					"INSERT INTO driver_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (8,7,'content4',1.2);",
				}
				s.execSql(t, sqls)
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
			rows, err := s.db.QueryContext(context.Background(), tc.sql)
			require.NoError(t, err)
			tc.after(t, rows)
			// 清理数据
			s.clearTable(t)
		})
	}
}

func (s *shardingDriverTestSuite) TestDriver_CUD() {
	t := s.T()
	testcases := []struct {
		name   string
		sql    string
		before func(t *testing.T)
		after  func(t *testing.T)
	}{
		// Insert
		{
			name:   "插入多条数据",
			sql:    "INSERT INTO `order` (`user_id`,`order_id`,`content`,`account`) values (1,3,'content',1.1),(2,4,'content4',1.3),(3,3,'content3',1.3);",
			before: func(t *testing.T) {},
			after: func(t *testing.T) {
				t.Helper()
				rows := s.getRowsFromTable(t, []int64{1, 2, 3})
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
				actualOrderList := s.getColsFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, actualOrderList)
			},
		},
		// Update
		{
			name: "更新一行",
			sql:  "UPDATE `order` SET `order_id` = 3,`content`='content',`account`=1.6 WHERE `user_id` = 1;",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO order_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (2,4,'content4',1.3);",
					"INSERT INTO order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (1,1,'content1',1.1);",
				}
				s.execSql(t, sqls)
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := s.getRowsFromTable(t, []int64{1, 2})
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
				orders := s.getColsFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		{
			name: "更新多行",
			sql:  "UPDATE `order` SET `order_id` = 3,`content`='content',`account`=1.6 WHERE `user_id` in (1,2);",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO order_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (2,4,'content4',1.3);",
					"INSERT INTO order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (1,1,'content1',1.1);",
					"INSERT INTO order_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (3,1,'content1',1.1);",
				}
				s.execSql(t, sqls)
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := s.getRowsFromTable(t, []int64{1, 2, 3})
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
				orders := s.getColsFromRows(t, rows)
				assert.ElementsMatch(t, wantOrderList, orders)
			},
		},
		// Delete
		{
			name: "删除一行",
			sql:  "DELETE FROM `order` WHERE `user_id` = 1;",
			before: func(t *testing.T) {
				t.Helper()
				sqls := []string{
					"INSERT INTO order_db_2.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (2,4,'content4',1.3);",
					"INSERT INTO order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (1,1,'content1',1.1);",
					"INSERT INTO order_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) VALUES (3,1,'content1',1.1);",
				}
				s.execSql(t, sqls)
			},
			after: func(t *testing.T) {
				t.Helper()
				rows := s.getRowsFromTable(t, []int64{1, 2, 3})
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
				orders := s.getColsFromRows(t, rows)
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
			s.clearTable(t)
		})
	}
}

func (s *shardingDriverTestSuite) clearTable(t *testing.T) {
	t.Helper()
	for i := 0; i < 3; i++ {
		_, err := s.db.Exec(fmt.Sprintf("DELETE FROM `driver_db_%d.order_tab`;", i))
		require.NoError(t, err)
	}
}

func (s *shardingDriverTestSuite) getRowsFromTable(t *testing.T, ids []int64) *sql.Rows {
	t.Helper()
	idStr := make([]string, 0, len(ids))
	for _, id := range ids {
		idStr = append(idStr, strconv.FormatInt(id, 10))
	}
	query := fmt.Sprintf("SELECT /* useMaster */ `user_id`, `order_id`, `content`, `account` FROM `order_tab` WHERE `user_id` in (%s)", strings.Join(idStr, ","))
	rows, err := s.db.Query(query)
	require.NoError(t, err)
	return rows
}

func (s *shardingDriverTestSuite) execSql(t *testing.T, sqls []string) {
	t.Helper()
	for _, vsql := range sqls {
		_, err := s.db.Exec(vsql)
		require.NoError(t, err)
	}
}

func (s *shardingDriverTestSuite) getOrder(row *sql.Rows) (Order, error) {
	var order Order
	if row.Next() {
		err := row.Scan(&order.UserId, &order.OrderId, &order.Content, &order.Account)
		if err != nil {
			return Order{}, err
		}
	}
	return order, nil
}

func (s *shardingDriverTestSuite) getColsFromRows(t *testing.T, rows *sql.Rows) []Order {
	t.Helper()
	res := make([]Order, 0, 2)
	for rows.Next() {
		order := Order{}
		err := rows.Scan(&order.UserId, &order.OrderId, &order.Content, &order.Account)
		require.NoError(t, err)
		res = append(res, order)
	}
	return res
}
