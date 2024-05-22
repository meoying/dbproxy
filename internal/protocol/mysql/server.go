package mysql

import (
	"context"
	"database/sql"
	"github.com/meoying/dbproxy/internal/plugin/forward"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/cmd"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/connection"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
	"log/slog"
	"net"
	"sync"

	"github.com/ecodeclub/ekit/syncx"
	"github.com/hashicorp/go-multierror"
)

type Server struct {
	addr     string
	logger   *slog.Logger
	listener net.Listener

	conns     syncx.Map[uint32, *connection.Conn]
	executors map[byte]cmd.Executor

	// 关闭
	closeOnce sync.Once
}

// NewServer 暂时写死
func NewServer(addr string, db *sql.DB) *Server {
	return &Server{
		logger: slog.Default(),
		addr:   addr,
		executors: map[byte]cmd.Executor{
			cmd.CmdPing.Byte(): &cmd.PingExecutor{},
			cmd.CmdQuery.Byte(): cmd.NewQueryExecutor(&forward.Handler{
				DB: db,
			}),
		},
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = listener
	var id uint32 = 1
	for {
		rawConn, err1 := listener.Accept()
		if err1 != nil {
			return err1
		}
		conn := connection.NewConn(id, rawConn, s.omCmd)
		s.conns.Store(id, conn)
		id++
		go func() {
			// 关闭
			defer func() {
				s.conns.Delete(conn.Id)
				_ = conn.Close()
			}()
			err2 := conn.Loop()
			if err2 != nil {
				s.logger.Error("退出命令处理循环 %w", err2)
			}
		}()
	}
}

func (s *Server) omCmd(ctx context.Context, conn *connection.Conn, payload []byte) error {
	// 第一个字节是命令
	exec, ok := s.executors[payload[0]]
	if ok {
		cmdCtx := &cmd.Context{
			Context:         ctx,
			Conn:            conn,
			CapabilityFlags: conn.ClientCapabilityFlags(),
			CharacterSet:    conn.CharacterSet(),
		}
		return exec.Exec(cmdCtx, payload)
	}
	// 返回不支持的命令的响应
	err := conn.WritePacket(packet.BuildErrRespPacket(packet.ER_XAER_INVAL))
	return err
}

// Close 不需要设计成幂等的，因为调用者不存在误用的可能
func (s *Server) Close() error {
	var err error
	s.closeOnce.Do(func() {
		if s.listener != nil {
			err = multierror.Append(err, s.listener.Close())
		}
		// 目前只是关闭了 value，但是并没有删除掉对应的键值对
		s.conns.Range(func(key uint32, value *connection.Conn) bool {
			err1 := value.Close()
			err = multierror.Append(err, err1)
			return true
		})
	})
	return err
}
