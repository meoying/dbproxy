package cmd

import (
	"context"
	"fmt"
	"log"

	"github.com/ecodeclub/ekit/slice"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/connection"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/parser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

var _ Executor = &StmtExecuteExecutor{}

type StmtExecuteExecutor struct {
	hdl plugin.Handler
	*BaseStmtExecutor
}

func NewStmtExecuteExecutor(hdl plugin.Handler, executor *BaseStmtExecutor) *StmtExecuteExecutor {
	return &StmtExecuteExecutor{
		hdl:              hdl,
		BaseStmtExecutor: executor,
	}
}

func (e *StmtExecuteExecutor) Exec(
	ctx context.Context,
	conn *connection.Conn,
	payload []byte) error {

	stmtId := e.parseStmtID(payload)
	args, err := e.parseArgs(conn.ClientCapabilityFlags(), stmtId, payload)
	if err != nil {
		return e.writeErrRespPacket(conn, err)
	}
	executeStmtSQL := e.generateExecuteStmtSQL(stmtId)

	pctx := &pcontext.Context{
		Context:     ctx,
		ParsedQuery: pcontext.NewParsedQuery(executeStmtSQL),
		Args:        args,
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

	return e.handlePluginResult(result, conn, e.handlePrepareSQLRows)
}

func (e *StmtExecuteExecutor) parseArgs(clientCapabilityFlags flags.CapabilityFlags, stmtID uint32, payload []byte) ([]any, error) {
	numParams, ok := e.loadNumParams(stmtID)

	log.Printf("loadNumParams stmtID = %d, numParams = %d", stmtID, numParams)
	if !ok {
		return nil, fmt.Errorf("failed to load num params")
	}

	p := parser.NewStmtExecutePacket(clientCapabilityFlags, numParams)
	if err := p.Parse(payload); err != nil {
		return nil, err
	}

	return slice.Map(p.Parameters(), func(idx int, src parser.StmtExecuteParameter) any {
		log.Printf("get execute params[%d] = %#v\n", idx, src)
		return src.Value
	}), nil
}
