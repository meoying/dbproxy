//go:build e2e

package test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/meoying/dbproxy/internal/protocol/mysql/configbuilder"
	"github.com/meoying/dbproxy/test/testsuite"
	"github.com/stretchr/testify/suite"
)

// TestDockerDBProxy 测试docker部署形态的dbproxy
func TestDockerDBProxy(t *testing.T) {
	t.Run("TestForwardPlugin", func(t *testing.T) {
		suite.Run(t, &dockerForwardTestSuite{})
	})
	t.Run("TestShardingPlugin", func(t *testing.T) {
		suite.Run(t, new(dockerShardingTestSuite))
	})
}

// dockerForwardTestSuite 用于测试启用Forward插件的docker dbproxy
type dockerForwardTestSuite struct {
	suite.Suite
	dataTypeSuite testsuite.DataTypeTestSuite
	basicSuite    testsuite.BasicTestSuite
	singleTxSuite testsuite.SingleTXTestSuite
}

func (s *dockerForwardTestSuite) SetupSuite() {
	s.createDatabasesAndTables()
	s.setupTestSuites()
}

func (s *dockerForwardTestSuite) createDatabasesAndTables() {
	t := s.T()
	testsuite.CreateTables(t, s.newMySQLDB())
}

func (s *dockerForwardTestSuite) newMySQLDB() *sql.DB {
	// TODO 暂不支持 ?charset=utf8mb4&parseTime=True&loc=Local
	db, err := sql.Open("mysql", "root:root@tcp(localhost:13306)/dbproxy")
	s.NoError(err)
	return db
}

func (s *dockerForwardTestSuite) setupTestSuites() {
	s.dataTypeSuite.SetProxyDBAndMySQLDB(s.newProxyClientDB(), s.newMySQLDB())
	s.basicSuite.SetDB(s.newProxyClientDB())
	// // TODO 修复Bug后需要使用下方代码
	s.singleTxSuite.SetDB(s.newProxyClientDB())
	// // TODO 绕过Bug使用标准driver包来验证测试用例集的有效性,修复Bug后需要关闭下方代码
	// s.singleTxSuite.SetDB(s.newMySQLDB())
}

func (s *dockerForwardTestSuite) newProxyClientDB() *sql.DB {
	// TODO 暂不支持 ?charset=utf8mb4&parseTime=True&loc=Local
	db, err := sql.Open("mysql", "root:root@tcp(localhost:8308)/dbproxy")
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
	suite.Run(s.T(), &s.dataTypeSuite)
}

func (s *dockerForwardTestSuite) TestBasicSuite() {
	suite.Run(s.T(), &s.basicSuite)
}

func (s *dockerForwardTestSuite) TestSingleTxSuite() {
	t := s.T()
	// t.Skip("暂不支持插件形态下的事务,事务类型选择应该是hint形式,报错: Error 1398 (HY000): Internal error: sql: transaction has already been committed or rolled back")
	suite.Run(t, &s.singleTxSuite)
}

// dockerShardingTestSuite 用于测试启用Sharding插件的docker dbproxy
type dockerShardingTestSuite struct {
	suite.Suite
	basicSuite        testsuite.BasicTestSuite
	distributeTxSuite testsuite.DistributeTXTestSuite
}

func (s *dockerShardingTestSuite) SetupSuite() {
	s.createDatabasesAndTables()
	s.setupTestSuites()
}

func (s *dockerShardingTestSuite) createDatabasesAndTables() {
	t := s.T()

	builder := configbuilder.ShardingConfigBuilder{}
	path, err := getAbsPath("/testdata/config/local/plugin/sharding.yaml")
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

func (s *dockerShardingTestSuite) setupTestSuites() {
	s.basicSuite.SetDB(s.newProxyClientDB())
	s.distributeTxSuite.SetDB(s.newProxyClientDB())
}

func (s *dockerShardingTestSuite) newProxyClientDB() *sql.DB {
	// TODO 暂不支持 ?charset=utf8mb4&parseTime=True&loc=Local
	db, err := sql.Open("mysql", "root:root@tcp(localhost:8308)/dbproxy")
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
	suite.Run(s.T(), &s.basicSuite)
}

func (s *dockerShardingTestSuite) TestDistributeTxSuite() {
	t := s.T()
	t.Skip("协议暂不支持开启事务、提交事务、回滚事务, 另外事务类型的选取方式应该是hint而不是ctx")
	suite.Run(t, &s.distributeTxSuite)
}
