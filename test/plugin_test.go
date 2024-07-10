//go:build e2e

package test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/meoying/dbproxy/internal/datasource/single"
	"github.com/meoying/dbproxy/internal/protocol/mysql"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/forward"
	"github.com/meoying/dbproxy/test/testsuite"
	"github.com/stretchr/testify/suite"
)

func TestLocalForwardDBProxy(t *testing.T) {
	// TODO 定义配置文件
	suite.Run(t, new(localForwardDBProxyTestSuite))
}

type localForwardDBProxyTestSuite struct {
	server *mysql.Server
	suite.Suite
	protocolSuite testsuite.ProtocolTestSuite
}

func (s *localForwardDBProxyTestSuite) SetupSuite() {
	s.setupProxyServer()
	s.setupProtocolTestSuite()
}

func (s *localForwardDBProxyTestSuite) setupProxyServer() {
	// client <-> dbproxy server <-> MYSQL DB, 此处db为dbproxy server与MySQL之间的连接
	db, err := single.OpenDB("mysql", "root:root@tcp(localhost:13306)/dbproxy")
	s.NoError(err)

	f := forward.NewPlugin(forward.NewHandler(db))
	// TODO: 定义文件地址, 读取文件
	// s.Equal("forward", f.Name())
	// s.NoError(f.Init([]byte("")))
	plugins := []plugin.Plugin{
		f,
	}
	s.server = mysql.NewServer(":8306", plugins)

	go func() {
		s.NoError(s.server.Start())
	}()
}

func (s *localForwardDBProxyTestSuite) setupProtocolTestSuite() {
	proxyDB, err := s.newProxyDB()
	s.NoError(err)
	mysqlDB, err := sql.Open("mysql", "root:root@tcp(localhost:13306)/dbproxy")
	s.NoError(err)
	s.protocolSuite.SetProxyDBAndMySQLDB(proxyDB, mysqlDB)
}

func (s *localForwardDBProxyTestSuite) newProxyDB() (*sql.DB, error) {
	return sql.Open("mysql", "root:root@tcp(localhost:8306)/dbproxy")
}

func (s *localForwardDBProxyTestSuite) TearDownSuite() {
	s.NoError(s.server.Close())
}

// TestPing
// TODO: 当driver形态支持PingContext后将此测试移动到[testsuite.CRUDTestSuite]
func (s *localForwardDBProxyTestSuite) TestPing() {
	proxyDB, err := s.newProxyDB()
	s.NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	s.NoError(proxyDB.PingContext(ctx))
}

func (s *localForwardDBProxyTestSuite) TestProtocolSuite() {
	suite.Run(s.T(), &s.protocolSuite)
}

func TestLocalShardingDBProxy(t *testing.T) {

}

type localShardingDBProxy struct {
}
