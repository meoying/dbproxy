package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/meoying/dbproxy/internal/datasource/single"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/forward"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ServerTestSuite struct {
	suite.Suite
	server *Server
	realDB *single.DB
}

func (s *ServerTestSuite) SetupSuite() {
	// 这里用真实的 DB，因为你要转发过去来测试
	db, err := single.OpenDB("mysql", "root:root@tcp(localhost:13306)/dbproxy")
	require.NoError(s.T(), err)
	hdl := forward.NewHandler(db)
	plugins := []plugin.Plugin{
		forward.NewPlugin(hdl),
	}
	server := NewServer(":8306", plugins)
	s.realDB = db
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

func (s *ServerTestSuite) TestSelect() {
	db, err := sql.Open("mysql", "root:root@tcp(localhost:8306)/mysql")
	require.NoError(s.T(), err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT * FROM users WHERE id = 1")
	require.NoError(s.T(), err)
	for rows.Next() {
		// 在这里读取并且打印数据
		// 假设你只有 id 和 name 两个列
		var id int64
		var name string
		err = rows.Scan(&id, &name)
		require.NoError(s.T(), err)
		s.T().Log(id, name)
	}
}

func (s *ServerTestSuite) TestInsert() {
	db, err := sql.Open("mysql", "root:root@tcp(localhost:8306)/mysql")
	require.NoError(s.T(), err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := db.ExecContext(ctx, "insert into users(name) VALUES ('Andy')")
	require.NoError(s.T(), err)
	s.T().Log(result)
}

func (s *ServerTestSuite) TestUpdate() {
	db, err := sql.Open("mysql", "root:root@tcp(localhost:8306)/mysql")
	require.NoError(s.T(), err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := db.ExecContext(ctx, "update users set name='Jack' where name = 'Andy'")
	require.NoError(s.T(), err)
	s.T().Log(result)
}

func (s *ServerTestSuite) TestDelete() {
	db, err := sql.Open("mysql", "root:root@tcp(localhost:8306)/mysql")
	require.NoError(s.T(), err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := db.ExecContext(ctx, "delete from users where name='Jack'")
	require.NoError(s.T(), err)
	s.T().Log(result)
}

func (s *ServerTestSuite) TestTransaction() {
	db, err := sql.Open("mysql", "root:root@tcp(localhost:8306)/mysql")
	require.NoError(s.T(), err)
	//ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	//defer cancel()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		s.T().Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = tx.ExecContext(ctx, "insert into users(name) VALUES ('Harry')")
	if err != nil {
		s.T().Fatalf("Failed to begin transaction: %v", err)
	}

	err = tx.Commit()
	//err = tx.Rollback()
	if err != nil {
		s.T().Fatalf("Failed to commit: %v", err)
	}
}

func TestServer(t *testing.T) {
	suite.Run(t, new(ServerTestSuite))
}
