package cmd

import (
	"context"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/connection"
)

type Executor interface {
	// Exec 执行命令，并且返回响应
	// 传入的 payload 部分不包含 packet 的头部字段
	Exec(ctx context.Context, conn *connection.Conn, payload []byte) error
}
