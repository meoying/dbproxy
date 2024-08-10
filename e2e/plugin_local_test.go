//go:build e2e

package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/meoying/dbproxy/e2e/testsuite"
	"github.com/meoying/dbproxy/internal/protocol/mysql"
	"github.com/meoying/dbproxy/internal/protocol/mysql/configbuilder"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/forward"
	logplugin "github.com/meoying/dbproxy/internal/protocol/mysql/plugin/log"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/sharding"
	"github.com/stretchr/testify/suite"
)

// TestLocalDBProxy 测试本地部署形态的dbproxy
func TestLocalDBProxy(t *testing.T) {
	t.Run("TestForwardPlugin", func(t *testing.T) {
		suite.Run(t, &localForwardTestSuite{serverAddress: "localhost:8306"})
	})
	t.Run("TestShardingPlugin", func(t *testing.T) {
		suite.Run(t, &localShardingTestSuite{serverAddress: "localhost:8307"})
	})
}

// localForwardTestSuite 用于测试启用Forward插件的本地dbproxy
type localForwardTestSuite struct {
	server        *mysql.Server
	serverAddress string
	suite.Suite
}

func (s *localForwardTestSuite) SetupSuite() {
	s.createDatabasesAndTables()
	s.setupProxyServer()
}

func (s *localForwardTestSuite) createDatabasesAndTables() {
	t := s.T()
	testsuite.CreateTables(t, s.newMySQLDB())
}

func (s *localForwardTestSuite) newMySQLDB() *sql.DB {
	// TODO 暂不支持 ?charset=utf8mb4&parseTime=True&loc=Local
	db, err := sql.Open("mysql", fmt.Sprintf(testsuite.MYSQLDSNTmpl, "dbproxy"))
	s.NoError(err)

	// 下方为调试时使用
	// customLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	// connector, err := driverlog.NewConnector(&mysqldriver.MySQLDriver{},
	// 	fmt.Sprintf(testsuite.MYSQLDSNTmpl, "dbproxy"),
	// 	driverlog.WithLogger(customLogger))
	// s.NoError(err)
	// db := sql.OpenDB(connector)
	return db
}

func (s *localForwardTestSuite) setupProxyServer() {
	s.server = mysql.NewServer(s.serverAddress, []plugin.Plugin{
		s.getLogPlugin("/testdata/config/local/plugins/forward_log.yaml"),
		s.getForwardPlugin("/testdata/config/local/plugins/forward.yaml"),
	})
	go func() {
		s.NoError(s.server.Start())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.newProxyClientDB().PingContext(ctx)
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

func (s *localForwardTestSuite) newProxyClientDB() *sql.DB {
	// TODO 暂不支持 ?charset=utf8mb4&parseTime=True&loc=Local
	db, err := sql.Open("mysql", fmt.Sprintf("root:root@tcp(%s)/dbproxy", s.serverAddress))
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
	var dataTypeSuite testsuite.DataTypeTestSuite
	dataTypeSuite.SetProxyDBAndMySQLDB(s.newProxyClientDB(), s.newMySQLDB())
	suite.Run(s.T(), &dataTypeSuite)
}

func (s *localForwardTestSuite) TestBasicSuite() {
	var basicSuite testsuite.BasicTestSuite
	basicSuite.SetDB(s.newProxyClientDB())
	suite.Run(s.T(), &basicSuite)
}

func (s *localForwardTestSuite) TestSingleTxSuite() {
	// 因为是并发测试,所以放在最后
	t := s.T()
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
	// 清理数据库表
	testsuite.ClearTables(t, s.newProxyClientDB())
}

func (s *localForwardTestSuite) TestPrepareDataTypeSuite() {
	var prepareStatementDataTypeTestSuite testsuite.PrepareDataTypeTestSuite
	prepareStatementDataTypeTestSuite.SetProxyDBAndMySQLDB(s.newProxyClientDB(), s.newMySQLDB())
	suite.Run(s.T(), &prepareStatementDataTypeTestSuite)
}

func (s *localForwardTestSuite) TestPrepareBasicSuite() {
	var prepareBasicTestSuite testsuite.PrepareBasicTestSuite
	prepareBasicTestSuite.SetDB(s.newProxyClientDB())
	suite.Run(s.T(), &prepareBasicTestSuite)
}

func (s *localForwardTestSuite) TestPrepareSingleTxSuite() {
	// 因为是并发测试,所以放在最后
	t := s.T()
	var wg sync.WaitGroup
	for id, txSuite := range []*testsuite.PrepareSingleTXTestSuite{
		new(testsuite.PrepareSingleTXTestSuite),
		new(testsuite.PrepareSingleTXTestSuite),
		new(testsuite.PrepareSingleTXTestSuite),
	} {
		wg.Add(1)
		id := id + 4
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
	// 清理数据库表
	testsuite.ClearTables(t, s.newProxyClientDB())
}

// localShardingTestSuite 用于测试启用Sharding插件的本地dbproxy
type localShardingTestSuite struct {
	server        *mysql.Server
	serverAddress string
	suite.Suite
}

func (s *localShardingTestSuite) SetupSuite() {
	s.createDatabasesAndTables()
	s.setupProxyServer()
}

func (s *localShardingTestSuite) createDatabasesAndTables() {
	t := s.T()

	builder := configbuilder.ShardingConfigBuilder{}
	path, err := getAbsPath("/testdata/config/local/plugins/sharding.yaml")
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
	s.server = mysql.NewServer(s.serverAddress, []plugin.Plugin{
		s.getLogPlugin("/testdata/config/local/plugins/sharding_log.yaml"),
		s.getShardingPlugin("/testdata/config/local/plugins/sharding.yaml"),
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

func (s *localShardingTestSuite) newProxyClientDB() *sql.DB {
	// TODO 暂不支持 ?charset=utf8mb4&parseTime=True&loc=Local
	db, err := sql.Open("mysql", fmt.Sprintf("root:root@tcp(%s)/local_sharding_plugin_db", s.serverAddress))
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
	var basicSuite testsuite.BasicTestSuite
	basicSuite.SetDB(s.newProxyClientDB())
	suite.Run(s.T(), &basicSuite)
}

func (s *localShardingTestSuite) TestDistributeTxSuite() {
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
