package cmd

import (
	"context"
	"fmt"
	"log"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/connection"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/builder"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

var (
	_ Executor = &StmtPrepareExecutor{}
)

type StmtPrepareExecutor struct {
	hdl plugin.Handler
	*BaseStmtExecutor
}

func NewStmtPrepareExecutor(hdl plugin.Handler, executor *BaseStmtExecutor) *StmtPrepareExecutor {
	return &StmtPrepareExecutor{
		hdl:              hdl,
		BaseStmtExecutor: executor,
	}
}

func (e *StmtPrepareExecutor) Exec(
	ctx context.Context,
	conn *connection.Conn,
	payload []byte) error {

	query := e.parseQuery(payload)
	stmtID := e.generateStmtID()
	numParams := e.storeNumParams(stmtID, query)

	prepareStmtSQL := e.generatePrepareStmtSQL(stmtID, query)

	log.Printf("Query = %s\n", query)
	log.Printf("PrepareStmtSQL = %s\n", prepareStmtSQL)

	pctx := &pcontext.Context{
		Context:     ctx,
		Query:       query,
		ParsedQuery: pcontext.NewParsedQuery(prepareStmtSQL),
		ConnID:      conn.ID(),
		StmtID:      stmtID,
	}

	// 在这里执行 que，并且写回响应
	result, err := e.hdl.Handle(pctx)
	if err != nil {
		// 回写错误响应
		// 先返回系统错误
		return e.writeErrRespPacket(conn, err)
	}

	conn.SetInTransaction(result.InTransactionState)

	return e.writeRespPackets(conn, e.buildRespPackets(stmtID, numParams, conn))
}

// generatePrepareStmtSQL 获取创建prepare的sql语句
func (e *StmtPrepareExecutor) generatePrepareStmtSQL(stmtId uint32, query string) string {
	return fmt.Sprintf("PREPARE stmt%d FROM '%s'", stmtId, query)
}

func (e *StmtPrepareExecutor) buildRespPackets(stmtID uint32, numParams uint64, conn *connection.Conn) [][]byte {
	b := builder.NewStmtPrepareOKPacket(conn.ClientCapabilityFlags(), e.getServerStatus(conn), conn.CharacterSet())
	b.StatementID = stmtID
	b.NumParams = uint16(numParams)
	return b.Build()
}
