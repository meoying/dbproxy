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
		data, err := c.readPacket()
		require.NoError(t, err)
		println(data)
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

func TestConnManualTestSuite(t *testing.T) {
	suite.Run(t, new(ConnManualTestSuite))
}
