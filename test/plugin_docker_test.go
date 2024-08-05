//go:build e2e

package test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/meoying/dbproxy/internal/protocol/mysql/configbuilder"
	"github.com/meoying/dbproxy/test/testsuite"
	"github.com/stretchr/testify/suite"
)

// TestDockerDBProxy 测试docker部署形态的dbproxy
func TestDockerDBProxy(t *testing.T) {
	t.Run("TestForwardPlugin", func(t *testing.T) {
		suite.Run(t, &dockerForwardTestSuite{serverAddress: "localhost:8308"})
	})
	t.Run("TestShardingPlugin", func(t *testing.T) {
		suite.Run(t, &dockerShardingTestSuite{serverAddress: "localhost:8309"})
	})
}

// dockerForwardTestSuite 用于测试启用Forward插件的docker容器dbproxy-forward
type dockerForwardTestSuite struct {
	suite.Suite
	serverAddress string
}

func (s *dockerForwardTestSuite) SetupSuite() {
	s.createDatabasesAndTables()
}

func (s *dockerForwardTestSuite) createDatabasesAndTables() {
	t := s.T()
	testsuite.CreateTables(t, s.newMySQLDB())
}

func (s *dockerForwardTestSuite) newMySQLDB() *sql.DB {
	// TODO 暂不支持 ?charset=utf8mb4&parseTime=True&loc=Local
	db, err := sql.Open("mysql", fmt.Sprintf(testsuite.MYSQLDSNTmpl, "dbproxy"))
	s.NoError(err)
	return db
}

func (s *dockerForwardTestSuite) newProxyClientDB() *sql.DB {
	// TODO 暂不支持 ?charset=utf8mb4&parseTime=True&loc=Local
	// 与/testdata/config/docker/dbproxy-forward.yaml中的服务端口一致
	db, err := sql.Open("mysql", fmt.Sprintf("root:root@tcp(%s)/dbproxy", s.serverAddress))
	s.NoError(err)
	return db
}

// TestPing
// TODO: 当driver形态支持PingContext后将此测试移动到[testsuite.BasicTestSuite]
func (s *dockerForwardTestSuite) TestPing() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.NoError(s.newProxyClientDB().PingContext(ctx))
}

func (s *dockerForwardTestSuite) TestDataTypeSuite() {
	var dataTypeSuite testsuite.DataTypeTestSuite
	dataTypeSuite.SetProxyDBAndMySQLDB(s.newProxyClientDB(), s.newMySQLDB())
	suite.Run(s.T(), &dataTypeSuite)
}

func (s *dockerForwardTestSuite) TestBasicSuite() {
	t := s.T()
	t.Skip("TODO: 没有发布支持prepare的版本, 所以暂时跳过")
	var basicSuite testsuite.BasicTestSuite
	basicSuite.SetDB(s.newProxyClientDB())
	suite.Run(t, &basicSuite)
}

func (s *dockerForwardTestSuite) TestSingleTxSuite() {
	// 因为是并发测试,所以放在最后
	t := s.T()
	t.Skip("TODO: 没有发布支持prepare的版本, 所以暂时跳过")
	var wg sync.WaitGroup
	for id, txSuite := range []*testsuite.SingleTXTestSuite{
		new(testsuite.SingleTXTestSuite),
		new(testsuite.SingleTXTestSuite),
		new(testsuite.SingleTXTestSuite),
	} {
		wg.Add(1)
		id := id + 1
		clientID := id * 10
		txSuite := txSuite
		txSuite.SetClientID(clientID)
		txSuite.SetDB(s.newProxyClientDB())
		go func() {
			defer wg.Done()
			t.Run(fmt.Sprintf("客户端-%d", clientID), func(t *testing.T) {
				suite.Run(t, txSuite)
			})
		}()
	}
	wg.Wait()
}

// dockerShardingTestSuite 用于测试启用Sharding插件的docker容器dbproxy-sharding
type dockerShardingTestSuite struct {
	suite.Suite
	serverAddress string
}

func (s *dockerShardingTestSuite) SetupSuite() {
	s.createDatabasesAndTables()
}

func (s *dockerShardingTestSuite) createDatabasesAndTables() {
	t := s.T()

	builder := configbuilder.ShardingConfigBuilder{}
	path, err := getAbsPath("/testdata/config/docker/plugins/sharding.yaml")

	s.NoError(err)
	err = builder.LoadConfigFile(path)
	s.NoError(err)

	config := builder.Config()

	// 该方法中的各种*sql.DB仅是用来创建节点机器上的数据库(mysql中的库概念)和库中的数据表
	clusterDB := testsuite.WaitForMySQLSetup(s.newDSN(""))

	hash := config.Algorithm.Hash
	dbBase := hash.DBPattern.Base
	dbPattern := hash.DBPattern.Name
	tablePattern := hash.TBPattern.Name

	// 为节点创建数据库
	dbNames := make([]string, 0, dbBase)
	for i := 0; i < dbBase; i++ {
		dbNames = append(dbNames, fmt.Sprintf(dbPattern, i))
	}
	testsuite.CreateDatabases(t, clusterDB, dbNames...)
	s.NoError(clusterDB.Close())

	// 为节点创建数据表
	for _, name := range dbNames {
		d, er := testsuite.OpenSQLDB(s.newDSN(name))
		s.NoError(er)
		testsuite.CreateTables(t, d, tablePattern)
		s.NoError(d.Close())
	}
}

func (s *dockerShardingTestSuite) newDSN(name string) string {
	return fmt.Sprintf(testsuite.MYSQLDSNTmpl, name)
}

func (s *dockerShardingTestSuite) newProxyClientDB() *sql.DB {
	// TODO 暂不支持 ?charset=utf8mb4&parseTime=True&loc=Local
	// 与/testdata/config/docker/dbproxy-sharding.yaml中的服务端口一致
	db, err := sql.Open("mysql", fmt.Sprintf("root:root@tcp(%s)/docker_sharding_plugin_db", s.serverAddress))
	s.NoError(err)
	return db
}

// TestPing
// TODO: 当driver形态支持PingContext后将此测试移动到[testsuite.BasicTestSuite]
func (s *dockerShardingTestSuite) TestPing() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.NoError(s.newProxyClientDB().PingContext(ctx))
}

func (s *dockerShardingTestSuite) TestBasicSuite() {
	t := s.T()
	t.Skip("TODO: 没有发布支持prepare的版本, 所以暂时跳过")
	var basicSuite testsuite.BasicTestSuite
	basicSuite.SetDB(s.newProxyClientDB())
	suite.Run(t, &basicSuite)
}

func (s *dockerShardingTestSuite) TestDistributeTxSuite() {
	// 因为是并发测试,所以放在最后
	t := s.T()
	var wg sync.WaitGroup
	for id, txSuite := range []*testsuite.DistributeTXTestSuite{
		new(testsuite.DistributeTXTestSuite),
		new(testsuite.DistributeTXTestSuite),
		new(testsuite.DistributeTXTestSuite),
	} {
		wg.Add(1)
		id := id + 1
		clientID := id * 100
		txSuite := txSuite
		txSuite.SetClientID(clientID)
		txSuite.SetDB(s.newProxyClientDB())
		go func() {
			defer wg.Done()
			t.Run(fmt.Sprintf("客户端-%d", clientID), func(t *testing.T) {
				suite.Run(t, txSuite)
			})
		}()
	}
	wg.Wait()
}
