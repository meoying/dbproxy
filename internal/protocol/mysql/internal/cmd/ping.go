package cmd

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
)

var _ Executor = &PingExecutor{}

// PingExecutor 负责处理 ping 的命令
type PingExecutor struct {
}

// Exec 默认返回处于 AutoCommit 状态
// TODO 由于调整了Executor结构这里也改了一下，需要看一下
func (p *PingExecutor) Exec(ctx *Context, payload []byte) error {
	return ctx.Conn.WritePacket(packet.BuildOKResp(packet.ServerStatusAutoCommit))
}
