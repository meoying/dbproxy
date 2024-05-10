package cmd

import (
	"gitee.com/meoying/dbproxy/internal/protocol/mysql/internal/consts"
	"gitee.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
)

var _ Executor = &PingExecutor{}

// PingExecutor 负责处理 ping 的命令
type PingExecutor struct {
}

// Exec 默认返回处于 AutoCommit 状态
func (p *PingExecutor) Exec(payload []byte) ([]byte, error) {
	return packet.BuildOKResp(consts.ServerStatusAutoCommit), nil
}
