//go:build manual

package mysql

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"net"
	"testing"
	"time"
)

// ConnManualTestSuite 手动运行的测试，用来开发调试
type ConnManualTestSuite struct {
	suite.Suite
}

func (s *ConnManualTestSuite) TestStartServer() {
	t := s.T()
	listener, err := net.Listen("tcp", ":8306")
	require.NoError(t, err)
	for {
		rc, err := listener.Accept()
		require.NoError(t, err)
		c := &Conn{
			conn:             rc,
			maxAllowedPacket: maxPacketSize,
			writeTimeout:     time.Second,
		}
		err = c.startHandshake()
		require.NoError(t, err)
		// 客户端响应了握手之后，会回复鉴权的必要信息
		// 在这里完成鉴权之后，返回一个 OK 的响应
		err = c.auth()
		require.NoError(t, err)

		// 用户发过来的查询请求
		data, err := c.readPacket()
		require.NoError(t, err)
		t.Log(data)
		// 返回结果
		err = c.writeOkPacket()
		require.NoError(t, err)
	}
}

func (s *ConnManualTestSuite) TestPingPong() {
	db, err := sql.Open("mysql", "root:root@tcp(localhost:8306)/mysql")
	require.NoError(s.T(), err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = db.PingContext(ctx)
	s.T().Log(err)
}

// TestRealPingPong 和真实的 DB 连接，用来查看 mysql 协议的细节
func (s *ConnManualTestSuite) TestRealPingPong() {
	db, err := sql.Open("mysql", "root:root@tcp(local.ubuntu:13316)/mysql")
	require.NoError(s.T(), err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = db.PingContext(ctx)
	s.T().Log(err)
}

func TestConnManualTestSuite(t *testing.T) {
	suite.Run(t, new(ConnManualTestSuite))
}
