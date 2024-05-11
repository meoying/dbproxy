package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ServerTestSuite struct {
	suite.Suite
	server *Server
}

func (s *ServerTestSuite) SetupSuite() {
	server := NewServer(":8306")
	s.server = server
	go func() {
		err := server.Start()
		s.T().Log(err)
	}()
}

func (s *ServerTestSuite) TearDownSuite() {
	err := s.server.Close()
	if err != nil {
		s.T().Log(err)
	}
}

func (s *ServerTestSuite) TestPingPong() {
	db, err := sql.Open("mysql", "root:root@tcp(localhost:8306)/mysql")
	require.NoError(s.T(), err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = db.PingContext(ctx)
	s.T().Log(err)
}

func TestServer(t *testing.T) {
	suite.Run(t, new(ServerTestSuite))
}
