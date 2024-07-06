package mysql

import (
	"context"
	"log/slog"
	"net"
	"sync"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/cmd"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/connection"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"

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

// NewServer
// 插件机制，需要进一步考虑细化
// 这里默认 plugin 已经完成了初始化
func NewServer(addr string, plugins []plugin.Plugin) *Server {
	var hdl plugin.Handler
	for i := len(plugins) - 1; i >= 0; i-- {
		hdl = plugins[i].Join(hdl)
	}
	return &Server{
		logger: slog.Default(),
		addr:   addr,
		executors: map[byte]cmd.Executor{
			cmd.CmdPing.Byte():        &cmd.PingExecutor{},
			cmd.CmdQuery.Byte():       cmd.NewQueryExecutor(hdl),
			cmd.CmdStmtPrepare.Byte(): cmd.NewStmtPrepareExecutor(hdl),
			cmd.CmdStmtExecute.Byte(): cmd.NewStmtExecuteExecutor(hdl),
			cmd.CmdStmtClose.Byte():   cmd.NewStmtCloseExecutor(hdl),
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
				s.logger.Error("退出命令处理循环 %w", "error", err2)
			}
		}()
	}
}

func (s *Server) omCmd(ctx context.Context, conn *connection.Conn, payload []byte) error {
	// 第一个字节是命令
	exec, ok := s.executors[payload[0]]
	if ok {
		return exec.Exec(ctx, conn, payload)
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
