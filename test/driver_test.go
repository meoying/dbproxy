//go:build e2e

package test

import (
	"database/sql"
	"fmt"
	"testing"

	shardingconfig "github.com/meoying/dbproxy/config/mysql/sharding"
	drviersharding "github.com/meoying/dbproxy/internal/protocol/mysql/driver/sharding"
	"github.com/meoying/dbproxy/test/testsuite"
	"github.com/stretchr/testify/suite"
)

// TestDriver 测试driver形态的dbproxy
func TestDriver(t *testing.T) {
	t.Run("TestShardingSuite", func(t *testing.T) {
		suite.Run(t, new(driverShardingTestSuite))
	})
}

// forwardDriverTestSuite 测试driver形体下的转发
type forwardDriverTestSuite struct {
	suite.Suite
	basicSuite    testsuite.BasicTestSuite
	singleTxSuite testsuite.SingleTXTestSuite
}

func (s *forwardDriverTestSuite) SetupSuite() {

}

func (s *forwardDriverTestSuite) TestBasicSuite() {
	suite.Run(s.T(), &s.basicSuite)
}

func (s *forwardDriverTestSuite) TestSingleTXSuite() {
	suite.Run(s.T(), &s.singleTxSuite)
}

// driverShardingTestSuite 测试分driver形态下的分库分表功能
type driverShardingTestSuite struct {
	suite.Suite
	basicSuite            testsuite.BasicTestSuite
	distributeTXTestSuite testsuite.DistributeTXTestSuite
}

func (s *driverShardingTestSuite) SetupSuite() {
	driverDB := s.setupDriverDB()
	s.basicSuite.SetDB(driverDB)
	s.distributeTXTestSuite.SetDB(driverDB)
}

func (s *driverShardingTestSuite) setupDriverDB() *sql.DB {
	path, err := getAbsPath("testdata/config/driver/sharding.yaml")
	s.NoError(err)

	cb := &drviersharding.ConnectorBuilder{}
	err = cb.LoadConfigFile(path)
	s.NoError(err)

	// 调整e2e后,有时需要调整createDBsAndTables中创建数据库的个数及数据表个数等
	s.createDBsAndTables(cb.Config())

	buildDB, err := cb.BuildDB()
	s.NoError(err)
	return buildDB
}

func (s *driverShardingTestSuite) createDBsAndTables(config shardingconfig.Config) {
	t := s.T()

	// 该方法中的各种*sql.DB仅是用来创建节点机器上的数据库(mysql中的库概念)和库中的数据表
	// 创建完成后就会被关闭,用户执行SQL只通过s.db
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

func (s *driverShardingTestSuite) newDSN(name string) string {
	return fmt.Sprintf(testsuite.MYSQLDSNTmpl, name)
}

func (s *driverShardingTestSuite) TestBasicSuite() {
	suite.Run(s.T(), &s.basicSuite)
}

func (s *driverShardingTestSuite) TestDistributeTXSuite() {
	suite.Run(s.T(), &s.distributeTXTestSuite)
}
