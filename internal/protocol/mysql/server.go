package mysql

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/cmd"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/connection"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/builder"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"

	"github.com/ecodeclub/ekit/syncx"
	"github.com/hashicorp/go-multierror"
)

type Server struct {
	addr     string
	logger   *slog.Logger
	mu       sync.Mutex
	listener net.Listener

	conns     syncx.Map[uint32, *connection.Conn]
	executors map[byte]cmd.Executor

	// 关闭
	closeOnce sync.Once
	closed    atomic.Bool
}

// NewServer
// 插件机制，需要进一步考虑细化
// 这里默认 plugin 已经完成了初始化
func NewServer(addr string, plugins []plugin.Plugin) *Server {
	var hdl plugin.Handler
	for i := len(plugins) - 1; i >= 0; i-- {
		hdl = plugins[i].Join(hdl)
	}

	baseExecutor := &cmd.BaseExecutor{}
	baseStmtExecutor := cmd.NewBaseStmtExecutor(baseExecutor)

	return &Server{
		logger: slog.Default(),
		addr:   addr,
		executors: map[byte]cmd.Executor{
			cmd.CmdPing.Byte():        &cmd.PingExecutor{},
			cmd.CmdQuery.Byte():       cmd.NewQueryExecutor(hdl, baseExecutor),
			cmd.CmdStmtPrepare.Byte(): cmd.NewStmtPrepareExecutor(hdl, baseStmtExecutor),
			cmd.CmdStmtExecute.Byte(): cmd.NewStmtExecuteExecutor(hdl, baseStmtExecutor),
			cmd.CmdStmtClose.Byte():   cmd.NewStmtCloseExecutor(hdl, baseStmtExecutor),
		},
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()
	var id uint32 = 1
	for {
		rawConn, err1 := listener.Accept()
		if err1 != nil {
			var opErr *net.OpError
			if errors.As(err, &opErr) && opErr.Temporary() {
				continue
			}
			if s.closed.Load() {
				// 忽略因为listener.Close()导致到err1
				return nil
			}
			return err1
		}
		conn := connection.NewConn(id, rawConn, s.omCmd)
		s.conns.Store(id, conn)
		id++
		go func() {
			// 关闭
			defer func() {
				s.conns.Delete(conn.ID())
				_ = conn.Close()
			}()
			err2 := conn.Loop()
			if err2 != nil {
				s.logger.Error("退出命令处理循环出错", "错误", err2)
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
	b := builder.ErrorPacketBuilder{
		ClientCapabilityFlags: conn.ClientCapabilityFlags(),
		Error:                 builder.ER_XAER_INVAL,
	}
	err := conn.WritePacket(b.Build())
	return err
}

// Close 不需要设计成幂等的，因为调用者不存在误用的可能
func (s *Server) Close() error {
	var err *multierror.Error
	s.closeOnce.Do(func() {
		s.closed.Store(true)

		s.mu.Lock()
		if s.listener != nil {
			err = multierror.Append(err, s.listener.Close())
		}
		s.mu.Unlock()

		// 目前只是关闭了 value，但是并没有删除掉对应的键值对
		s.conns.Range(func(key uint32, value *connection.Conn) bool {
			err = multierror.Append(err, value.Close())
			return true
		})
	})
	return err.ErrorOrNil()
}
