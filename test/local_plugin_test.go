//go:build e2e

package test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/meoying/dbproxy/internal/protocol/mysql"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/forward"
	logplugin "github.com/meoying/dbproxy/internal/protocol/mysql/plugin/log"
	"github.com/meoying/dbproxy/test/testsuite"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sync/errgroup"
)

// TestLocalDBProxy 测试本地部署形态的dbproxy
func TestLocalDBProxy(t *testing.T) {

	t.Run("TestForwardSuite", func(t *testing.T) {
		suite.Run(t, &localForwardDBProxyTestSuite{})
	})

	t.Run("TestShardingSuite", func(t *testing.T) {
		t.Skip()
		suite.Run(t, new(localShardingDBProxyTestSuite))
	})
}

// localForwardDBProxyTestSuite 用于测试启用Forward插件的本地dbproxy
type localForwardDBProxyTestSuite struct {
	server *mysql.Server
	suite.Suite
	dataTypeSuite testsuite.DataTypeTestSuite
	basicSuite    testsuite.BasicTestSuite
	singleTxSuite testsuite.SingleTXTestSuite
}

func (s *localForwardDBProxyTestSuite) SetupSuite() {
	s.createDatabasesAndTables()
	s.setupProxyServer()
	s.setupTestSuites()
}

func (s *localForwardDBProxyTestSuite) createDatabasesAndTables() {
	t := s.T()
	testsuite.CreateTables(t, s.newMySQLDB())
}

func (s *localForwardDBProxyTestSuite) newMySQLDB() *sql.DB {
	// TODO 暂不支持 ?charset=utf8mb4&parseTime=True&loc=Local
	db, err := sql.Open("mysql", "root:root@tcp(localhost:13306)/dbproxy")
	s.NoError(err)
	return db
}

func (s *localForwardDBProxyTestSuite) setupProxyServer() {
	s.server = mysql.NewServer(":8306", []plugin.Plugin{
		s.getLogPlugin("/testdata/config/local/plugin/log.yaml"),
		s.getForwardPlugin("/testdata/config/local/plugin/forward.yaml"),
	})
	go func() {
		s.NoError(s.server.Start())
	}()
}

func (s *localForwardDBProxyTestSuite) getForwardPlugin(path string) *forward.Plugin {
	f := &forward.Plugin{}
	s.Equal("forward", f.Name())
	config, err := getConfig(path)
	s.NoError(err)
	err = f.Init(config)
	s.NoError(err)
	return f
}

func (s *localForwardDBProxyTestSuite) getLogPlugin(path string) *logplugin.Plugin {
	l := &logplugin.Plugin{}
	s.Equal("log", l.Name())
	config, err := getConfig(path)
	s.NoError(err)
	err = l.Init(config)
	s.NoError(err)
	return l
}

func (s *localForwardDBProxyTestSuite) setupTestSuites() {
	s.dataTypeSuite.SetProxyDBAndMySQLDB(s.newProxyClientDB(), s.newMySQLDB())
	s.basicSuite.SetDB(s.newProxyClientDB())
	// // TODO dbproxy <---> MySQL 之间只有一个*sql.DB是不行的
	// // 应当拦截START Transaction,tx_id 创建 client_id_tx_01 := dbs[client_id].BeginTx 来执行事务
	// // 本质不能复用db *sql.DB, 多次调用tx := db.BeginTx(ctx,...) 来创建多个tx
	//
	// // TODO 修复Bug后需要使用下方代码
	// s.singleTxSuite.SetDB(s.newProxyClientDB())
	//
	// // TODO 绕过Bug使用标准driver包来验证测试用例集的有效性,修复Bug后需要关闭下方代码
	s.singleTxSuite.SetDB(s.newMySQLDB())
}

func (s *localForwardDBProxyTestSuite) newProxyClientDB() *sql.DB {
	// TODO 暂不支持 ?charset=utf8mb4&parseTime=True&loc=Local
	db, err := sql.Open("mysql", "root:root@tcp(localhost:13306)/dbproxy")
	s.NoError(err)
	return db
}

func (s *localForwardDBProxyTestSuite) TearDownSuite() {
	s.NoError(s.server.Close())
}

// TestPing
// TODO: 当driver形态支持PingContext后将此测试移动到[testsuite.BasicTestSuite]
func (s *localForwardDBProxyTestSuite) TestPing() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.NoError(s.newProxyClientDB().PingContext(ctx))
}

func (s *localForwardDBProxyTestSuite) TestDataTypeSuite() {
	suite.Run(s.T(), &s.dataTypeSuite)
}

func (s *localForwardDBProxyTestSuite) TestBasicSuite() {
	suite.Run(s.T(), &s.basicSuite)
}

func (s *localForwardDBProxyTestSuite) TestSingleTxSuite() {
	suite.Run(s.T(), &s.singleTxSuite)
}

// localShardingDBProxyTestSuite 用于测试启用Sharding插件的本地dbproxy
type localShardingDBProxyTestSuite struct {
	suite.Suite
	basicSuite        testsuite.BasicTestSuite
	distributeTxSuite testsuite.DistributeTXTestSuite
}

func (s *localShardingDBProxyTestSuite) SetupSuite() {
	// 启动server 8307
}

func (s *localShardingDBProxyTestSuite) TestBasicSuite() {
	suite.Run(s.T(), &s.basicSuite)
}

func (s *localShardingDBProxyTestSuite) TestDistributeTxSuite() {
	suite.Run(s.T(), &s.distributeTxSuite)
}

func TestShardingMySQLDB(t *testing.T) {
	t.Skip()
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
