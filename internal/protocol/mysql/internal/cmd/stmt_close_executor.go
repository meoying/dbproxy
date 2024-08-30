package cmd

import (
	"context"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/connection"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

var _ Executor = &StmtCloseExecutor{}

type StmtCloseExecutor struct {
	hdl plugin.Handler
	*BaseStmtExecutor
}

func NewStmtCloseExecutor(hdl plugin.Handler, executor *BaseStmtExecutor) *StmtCloseExecutor {
	return &StmtCloseExecutor{
		hdl:              hdl,
		BaseStmtExecutor: executor,
	}
}

func (e *StmtCloseExecutor) Exec(
	ctx context.Context,
	conn *connection.Conn,
	payload []byte) error {

	stmtId := e.parseStmtID(payload)
	deallocatePrepareStmtSQL := e.generateDeallocatePrepareStmtSQL(stmtId)
	pctx := &pcontext.Context{
		Context:     ctx,
		Query:       deallocatePrepareStmtSQL,
		ParsedQuery: pcontext.NewParsedQuery(deallocatePrepareStmtSQL),
		ConnID:      conn.ID(),
		StmtID:      stmtId,
	}

	// 在这里执行 que，并且写回响应
	result, err := e.hdl.Handle(pctx)
	if err != nil {
		// 回写错误响应
		// 先返回系统错误
		return e.writeErrRespPacket(conn, err)
	}

	// 重置conn的事务状态
	conn.SetInTransaction(result.InTransactionState)

	// 无需返回任何响应包给客户端
	// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_close.html
	return nil
}
