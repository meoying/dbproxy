package mysql

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/cluster"
	"github.com/meoying/dbproxy/internal/datasource/masterslave"
	"github.com/meoying/dbproxy/internal/datasource/shardingsource"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/sharding"
	"github.com/meoying/dbproxy/internal/sharding/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"testing"
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
		PRIMARY KEY (order_id)
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
	db, err := sql.Open("mysql", dsn)
	require.NoError(s.T(), err)
	// 创建db
	err = s.createDatabase(db, "order_db_0")
	require.NoError(s.T(), err)
	err = s.createDatabase(db, "order_db_1")
	require.NoError(s.T(), err)
	dsn0 := "root:root@tcp(127.0.0.1:13306)/order_db_0?charset=utf8mb4&parseTime=True&loc=Local"
	dsn1 := "root:root@tcp(127.0.0.1:13306)/order_db_1?charset=utf8mb4&parseTime=True&loc=Local"
	// 创建表
	db1, err := sql.Open("mysql", dsn0)
	require.NoError(s.T(), err)
	db2, err := sql.Open("mysql", dsn1)
	require.NoError(s.T(), err)
	err = s.createTable(db1, "order_tab")
	require.NoError(s.T(), err)
	err = s.createTable(db2, "order_tab")
	// 初始化
	dbBase := 2
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
	server := NewServer(":8306", plugins)
	s.db = db
	s.server = server
	go func() {
		err := server.Start()
		s.T().Log(err)
	}()
}

//func (s *TestShardingPluginSuite) TestSharding_Insert() {
//	db, err := sql.Open("mysql", "root:root@tcp(localhost:8306)/mysql")
//	require.NoError(s.T(), err)
//	sql2 := "insert into order (`user_id`,`order_id`,`content`,`account`) values (1,3,'content',1.1),(2,4,'content4',1.3);"
//	_, err = db.Exec(sql2)
//	require.NoError(s.T(), err)
//	row, err := s.db.Query("select * from order_db_1.order_tab where id = 1;")
//	require.NoError(s.T(), err)
//	row2, err := s.db.Query("select * from order_db_0.order_tab where id = 2;")
//	require.NoError(s.T(), err)
//	order1, err := s.getOrder(row)
//	require.NoError(s.T(), err)
//	order2, err := s.getOrder(row2)
//	require.NoError(s.T(), err)
//	assert.Equal(s.T(), []Order{
//		{
//			UserId:  1,
//			OrderId: 3,
//			Content: "content",
//			Account: 1.1,
//		},
//		{
//			UserId:  2,
//			OrderId: 4,
//			Content: "content4",
//			Account: 1.3,
//		},
//	}, []Order{
//		order1,
//		order2,
//	})
//
//}

func (s *TestShardingPluginSuite) TestSharding_Select() {
	//初始化数据
	sql1 := "insert into order_db_0.order_tab (`user_id`,`order_id`,`content`,`account`) values (2,4,'content4',1.3);"
	sql2 := "insert into order_db_1.order_tab (`user_id`,`order_id`,`content`,`account`) values (1,1,'content1',1.1);"
	_, err := s.db.Exec(sql1)
	require.NoError(s.T(), err)
	_, err = s.db.Exec(sql2)
	require.NoError(s.T(), err)
	//查询
	db, err := sql.Open("mysql", "root:root@tcp(localhost:8306)/mysql")
	require.NoError(s.T(), err)
	// 使用主库查找
	ctx := masterslave.UseMaster(context.Background())
	rows, err := db.QueryContext(ctx, "SELECT /* useMaster */   * FROM users WHERE (user_id = 1) or (user_id =2)")
	require.NoError(s.T(), err)
	res := make([]Order, 0, 2)
	for rows.Next() {
		order := Order{}
		err = rows.Scan(&order.UserId, &order.OrderId, &order.Content, &order.Account)
		require.NoError(s.T(), err)
		res = append(res, order)
	}
	assert.ElementsMatch(s.T(), []Order{
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
	deleteSql := "delete from  order_db_0.order_tab;"
	_, err = s.db.Exec(deleteSql)
	require.NoError(s.T(), err)
	deleteSql = "delete from  order_db_1.order_tab;"
	_, err = s.db.Exec(deleteSql)
	require.NoError(s.T(), err)
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

func (s *TestShardingPluginSuite) MasterSlavesMysqlDB(db *sql.DB) *masterslave.MasterSlavesDB {
	masterSlaveDB := masterslave.NewMasterSlavesDB(db)
	return masterSlaveDB
}

func TestTestShardingPluginSuite(t *testing.T) {
	suite.Run(t, new(TestShardingPluginSuite))
}
