package cmd

import (
	"context"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/connection"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

var _ Executor = &StmtCloseExecutor{}

type StmtCloseExecutor struct {
	hdl plugin.Handler
	*baseExecutor
}

func NewStmtCloseExecutor(hdl plugin.Handler) *StmtCloseExecutor {
	return &StmtCloseExecutor{
		hdl:          hdl,
		baseExecutor: &baseExecutor{},
	}
}

func (e *StmtCloseExecutor) Exec(
	ctx context.Context,
	conn *connection.Conn,
	payload []byte) error {
	stmtId := e.parsePrepareStmtID(payload)
	parseQue := e.getCloseStmtQuery(stmtId)
	pctx := &pcontext.Context{
		Context:     ctx,
		Query:       parseQue,
		ParsedQuery: pcontext.NewParsedQuery(parseQue, nil),
		ConnID:      conn.ID(),
		StmtID:      stmtId,
	}

	// 在这里执行 que，并且写回响应
	_, err := e.hdl.Handle(pctx)
	if err != nil {
		// 回写错误响应
		// 先返回系统错误
		errResp := packet.BuildErInternalError(err.Error())
		return conn.WritePacket(packet.BuildErrRespPacket(errResp))
	}

	// TODO 如果是插入、更新、删除行为应该把影响行数和最后插入ID给传进去
	return conn.WritePacket(packet.BuildOKResp(packet.ServerStatusAutoCommit))
}
