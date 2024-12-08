package cmd

import (
	"context"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/connection"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

var _ Executor = &QueryExecutor{}

type QueryExecutor struct {
	hdl plugin.Handler
	*BaseExecutor
}

func NewQueryExecutor(hdl plugin.Handler, executor *BaseExecutor) *QueryExecutor {
	return &QueryExecutor{
		hdl:          hdl,
		BaseExecutor: executor,
	}
}

// Exec
// Query 命令的 payload 格式在
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html
func (e *QueryExecutor) Exec(
	ctx context.Context,
	conn *connection.Conn,
	payload []byte) error {
	que := e.parseQuery(payload)
	pctx := &pcontext.Context{
		Context:     ctx,
		Query:       que,
		ParsedQuery: pcontext.NewParsedQuery(que),
		ConnID:      conn.ID(),
	}

	// 在这里执行 que，并且写回响应
	result, err := e.hdl.Handle(pctx)
	if err != nil {
		// 回写错误响应
		// 先返回系统错误
		return e.writeErrRespPacket(conn, err)
	}
	return e.handlePluginResult(result, conn, e.handleQuerySQLRows)
}
