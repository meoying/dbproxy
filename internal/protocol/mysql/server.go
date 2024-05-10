package mysql

import (
	"github.com/ecodeclub/ekit/syncx"
	"github.com/hashicorp/go-multierror"
	"log/slog"
	"net"
	"sync"
)

type Server struct {
	addr     string
	logger   *slog.Logger
	listener net.Listener

	conns syncx.Map[uint32, *Conn]

	// 关闭
	closeOnce sync.Once
}

func NewServer(addr string) *Server {
	return &Server{
		logger: slog.Default(),
		addr:   addr,
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
		conn := newConn(id, rawConn)
		s.conns.Store(id, conn)
		id++
		go func() {
			// 关闭
			defer func() {
				s.conns.Delete(conn.id)
				_ = conn.Close()
			}()
			err2 := conn.Loop()
			if err2 != nil {
				s.logger.Error("退出命令处理循环 %w", err2)
			}
		}()
	}
}

// Close 不需要设计成幂等的，因为调用者不存在误用的可能
func (s *Server) Close() error {
	var err error
	s.closeOnce.Do(func() {
		if s.listener != nil {
			err = multierror.Append(err, s.listener.Close())
		}

		// 目前只是关闭了 value，但是并没有删除掉对应的键值对
		s.conns.Range(func(key uint32, value *Conn) bool {
			err1 := value.Close()
			err = multierror.Append(err, err1)
			return true
		})
	})
	return err
}
