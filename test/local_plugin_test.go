//go:build e2e

package test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/meoying/dbproxy/internal/protocol/mysql"
	"github.com/meoying/dbproxy/internal/protocol/mysql/configbuilder"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/forward"
	logplugin "github.com/meoying/dbproxy/internal/protocol/mysql/plugin/log"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/sharding"
	"github.com/meoying/dbproxy/test/testsuite"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sync/errgroup"
)

// TestLocalDBProxy 测试本地部署形态的dbproxy
func TestLocalDBProxy(t *testing.T) {
	t.Run("TestForwardPlugin", func(t *testing.T) {
		suite.Run(t, &localForwardTestSuite{})
	})
	t.Run("TestShardingPlugin", func(t *testing.T) {
		suite.Run(t, new(localShardingTestSuite))
	})
}

// localForwardTestSuite 用于测试启用Forward插件的本地dbproxy
type localForwardTestSuite struct {
	server *mysql.Server
	suite.Suite
	dataTypeSuite testsuite.DataTypeTestSuite
	basicSuite    testsuite.BasicTestSuite
	singleTxSuite testsuite.SingleTXTestSuite
}

func (s *localForwardTestSuite) SetupSuite() {
	s.createDatabasesAndTables()
	s.setupProxyServer()
	s.setupTestSuites()
}

func (s *localForwardTestSuite) createDatabasesAndTables() {
	t := s.T()
	testsuite.CreateTables(t, s.newMySQLDB())
}

func (s *localForwardTestSuite) newMySQLDB() *sql.DB {
	// TODO 暂不支持 ?charset=utf8mb4&parseTime=True&loc=Local
	db, err := sql.Open("mysql", "root:root@tcp(localhost:13306)/dbproxy")
	s.NoError(err)
	return db
}

func (s *localForwardTestSuite) setupProxyServer() {
	s.server = mysql.NewServer(":8306", []plugin.Plugin{
		s.getLogPlugin("/testdata/config/local/plugin/log.yaml"),
		s.getForwardPlugin("/testdata/config/local/plugin/forward.yaml"),
	})
	go func() {
		s.NoError(s.server.Start())
	}()
}

func (s *localForwardTestSuite) getForwardPlugin(path string) *forward.Plugin {
	p := &forward.Plugin{}
	s.Equal("forward", p.Name())
	config, err := unmarshalConfigFile(path)
	s.NoError(err)
	err = p.Init(config)
	s.NoError(err)
	return p
}

func (s *localForwardTestSuite) getLogPlugin(path string) *logplugin.Plugin {
	p := &logplugin.Plugin{}
	s.Equal("log", p.Name())
	config, err := unmarshalConfigFile(path)
	s.NoError(err)
	err = p.Init(config)
	s.NoError(err)
	return p
}

func (s *localForwardTestSuite) setupTestSuites() {
	s.dataTypeSuite.SetProxyDBAndMySQLDB(s.newProxyClientDB(), s.newMySQLDB())
	s.basicSuite.SetDB(s.newProxyClientDB())
	// // TODO 修复Bug后需要使用下方代码
	s.singleTxSuite.SetDB(s.newProxyClientDB())
	// // TODO 绕过Bug使用标准driver包来验证测试用例集的有效性,修复Bug后需要关闭下方代码
	// s.singleTxSuite.SetDB(s.newMySQLDB())
}

func (s *localForwardTestSuite) newProxyClientDB() *sql.DB {
	// TODO 暂不支持 ?charset=utf8mb4&parseTime=True&loc=Local
	db, err := sql.Open("mysql", "root:root@tcp(localhost:8306)/dbproxy")
	s.NoError(err)
	return db
}

func (s *localForwardTestSuite) TearDownSuite() {
	s.NoError(s.server.Close())
}

// TestPing
// TODO: 当driver形态支持PingContext后将此测试移动到[testsuite.BasicTestSuite]
func (s *localForwardTestSuite) TestPing() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.NoError(s.newProxyClientDB().PingContext(ctx))
}

func (s *localForwardTestSuite) TestDataTypeSuite() {
	suite.Run(s.T(), &s.dataTypeSuite)
}

func (s *localForwardTestSuite) TestBasicSuite() {
	suite.Run(s.T(), &s.basicSuite)
}

func (s *localForwardTestSuite) TestSingleTxSuite() {
	t := s.T()
	t.Skip("暂不支持插件形态下的事务,事务类型选择应该是hint形式,报错: Error 1398 (HY000): Internal error: sql: transaction has already been committed or rolled back")
	suite.Run(t, &s.singleTxSuite)
}

// localShardingTestSuite 用于测试启用Sharding插件的本地dbproxy
type localShardingTestSuite struct {
	server *mysql.Server
	suite.Suite
	basicSuite        testsuite.BasicTestSuite
	distributeTxSuite testsuite.DistributeTXTestSuite
}

func (s *localShardingTestSuite) SetupSuite() {
	s.createDatabasesAndTables()
	s.setupProxyServer()
	s.setupTestSuites()
}

func (s *localShardingTestSuite) createDatabasesAndTables() {
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

func (s *localShardingTestSuite) newDSN(name string) string {
	return fmt.Sprintf(testsuite.MYSQLDSNTmpl, name)
}

func (s *localShardingTestSuite) setupProxyServer() {
	s.server = mysql.NewServer(":8307", []plugin.Plugin{
		s.getLogPlugin("/testdata/config/local/plugin/log.yaml"),
		s.getShardingPlugin("/testdata/config/local/plugin/sharding.yaml"),
	})
	go func() {
		s.NoError(s.server.Start())
	}()
}

func (s *localShardingTestSuite) getShardingPlugin(path string) *sharding.Plugin {
	p := &sharding.Plugin{}
	s.Equal("sharding", p.Name())
	config, err := unmarshalConfigFile(path)
	s.NoError(err)
	err = p.Init(config)
	s.NoError(err)
	return p
}

func (s *localShardingTestSuite) getLogPlugin(path string) *logplugin.Plugin {
	p := &logplugin.Plugin{}
	s.Equal("log", p.Name())
	config, err := unmarshalConfigFile(path)
	s.NoError(err)
	err = p.Init(config)
	s.NoError(err)
	return p
}

func (s *localShardingTestSuite) setupTestSuites() {
	s.basicSuite.SetDB(s.newProxyClientDB())
	s.distributeTxSuite.SetDB(s.newProxyClientDB())
}

func (s *localShardingTestSuite) newProxyClientDB() *sql.DB {
	// TODO 暂不支持 ?charset=utf8mb4&parseTime=True&loc=Local
	db, err := sql.Open("mysql", "root:root@tcp(localhost:8307)/dbproxy")
	s.NoError(err)
	return db
}

func (s *localShardingTestSuite) TearDownSuite() {
	s.NoError(s.server.Close())
}

// TestPing
// TODO: 当driver形态支持PingContext后将此测试移动到[testsuite.BasicTestSuite]
func (s *localShardingTestSuite) TestPing() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.NoError(s.newProxyClientDB().PingContext(ctx))
}

func (s *localShardingTestSuite) TestBasicSuite() {
	suite.Run(s.T(), &s.basicSuite)
}

func (s *localShardingTestSuite) TestDistributeTxSuite() {
	t := s.T()
	t.Skip("协议暂不支持开启事务、提交事务、回滚事务, 另外事务类型的选取方式应该是hint而不是ctx")
	suite.Run(t, &s.distributeTxSuite)
}

func TestMockForwardSingleTxProblem(t *testing.T) {
	t.Skip("模拟测试forward-singleTx的问题")
	// TODO 定义Sharding配置文件

	db, err := sql.Open("mysql", "root:root@tcp(localhost:13306)/dbproxy")
	require.NoError(t, err)

	testsuite.CreateTables(t, db, "order")
	defer testsuite.ClearTables(t, db)

	_, err = db.Exec("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (100000, 1001, 'sample content', 10.0);")
	require.NoError(t, err)

	n := 100
	var eg errgroup.Group
	for i := 0; i < n; i++ {
		i := i
		eg.Go(func() error {
			tx, err1 := db.BeginTx(context.Background(), nil)
			if err1 != nil {
				return err1
			}
			v := i % 3

			if v == 0 {
				// 插入
				_, err2 := tx.ExecContext(context.Background(), fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (10001%d, 1001, 'sample content', 10.0);", i))
				if err2 != nil {
					return err2
				}
				return tx.Commit()
			} else if v == 1 {
				// 修改
				_, err2 := tx.ExecContext(context.Background(), "UPDATE `order` SET `content` = 'tx content' WHERE `user_id`=100000")
				if err2 != nil {
					return err2
				}
				return tx.Commit()

			} else {
				// 删除
				_, err2 := tx.ExecContext(context.Background(), fmt.Sprintf("DELETE FROM `order` WHERE `user_id`=10000%d", i))
				if err2 != nil {
					return err2
				}
				return tx.Rollback()

			}
		})
		eg.Go(func() error {
			_, err = db.Exec(fmt.Sprintf("INSERT INTO `order` (`user_id`, `order_id`, `content`, `account`) VALUES (20000%d, 1001, 'sample content', 10.0);", i))
			return err
		})
	}

	require.NoError(t, eg.Wait())
}
