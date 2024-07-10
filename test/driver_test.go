//go:build e2e

package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/driver/sharding"
	"github.com/meoying/dbproxy/test/testsuite"
	"github.com/stretchr/testify/suite"
	"gopkg.in/yaml.v3"
)

// TestDriver 测试driver形态
func TestDriver(t *testing.T) {

	// TODO: forward 单个连接对应单个mysql
	// TODO: forward 对应的config, 我就想用

	t.Run("sharding", func(t *testing.T) {
		suite.Run(t, new(shardingDriverTestSuite))
	})

}

// shardingDriverTestSuite 只提供初始化测试集操作
type shardingDriverTestSuite struct {
	suite.Suite
	crudSuite             testsuite.CRUDTestSuite
	distributeTXTestSuite testsuite.DistributeTXTestSuite
}

func (s *shardingDriverTestSuite) SetupSuite() {
	yamlData, err := os.ReadFile("testdata/config/driver/sharding.yaml")
	s.NoError(err)

	var config sharding.Config
	err = yaml.Unmarshal(yamlData, &config)
	s.NoError(err)

	// 调整e2e后,有时需要调整createDBsAndTables中创建数据库的个数及数据表个数等
	s.createDBsAndTables(config)

	cb := &sharding.ConnectorBuilder{}
	cb.SetConfig(config)
	s.NoError(err)

	buildDB, err := cb.BuildDB()
	s.NoError(err)

	s.crudSuite.SetDB(buildDB)
	s.distributeTXTestSuite.SetDB(buildDB)
}

func (s *shardingDriverTestSuite) createDBsAndTables(config sharding.Config) {
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

func (s *shardingDriverTestSuite) newDSN(name string) string {
	return fmt.Sprintf("root:root@tcp(127.0.0.1:13306)/%s?charset=utf8mb4&parseTime=True&loc=Local", name)
}

func (s *shardingDriverTestSuite) TestCRUDSuite() {
	suite.Run(s.T(), &s.crudSuite)
}

func (s *shardingDriverTestSuite) TestDistributeTXSuite() {
	suite.Run(s.T(), &s.distributeTXTestSuite)
}
