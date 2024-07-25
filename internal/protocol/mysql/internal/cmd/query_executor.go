package cmd

import (
	"context"
	"database/sql"

	"github.com/ecodeclub/ekit/slice"
	"github.com/ecodeclub/ekit/sqlx"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/connection"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/vparser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

var _ Executor = &QueryExecutor{}

type QueryExecutor struct {
	hdl plugin.Handler
	*baseExecutor
}

func NewQueryExecutor(hdl plugin.Handler) *QueryExecutor {
	return &QueryExecutor{
		hdl:          hdl,
		baseExecutor: &baseExecutor{},
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
		ParsedQuery: pcontext.NewParsedQuery(que, vparser.NewHintVisitor()),
		ConnID:      conn.ID(),
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

	if result.Rows != nil {
		return e.handleRows(result.Rows, conn)
	}

	if result.InTransactionState {
		return conn.WritePacket(packet.BuildOKResp(packet.SeverStatusInTrans | packet.ServerStatusAutoCommit))
	}

	return e.writeOKRespPacket(conn)
}

func (e *QueryExecutor) handleRows(rows sqlx.Rows, conn *connection.Conn) error {
	cols, err := rows.ColumnTypes()
	if err != nil {
		return e.writeErrRespPacket(conn, err)
	}
	var data [][]any
	for rows.Next() {
		row := make([]any, len(cols))
		// 这里需要用到指针给Scan，不然会报错
		for i := range row {
			var v []byte
			row[i] = &v
		}
		err = rows.Scan(row...)
		if err != nil {
			return e.writeErrRespPacket(conn, err)
		}
		data = append(data, row)
	}

	columnTypes := slice.Map(cols, func(idx int, src *sql.ColumnType) packet.ColumnType {
		return src
	})
	respPackets, err := e.buildTextResultSetRespPackets(columnTypes, data, conn.CharacterSet())
	if err != nil {
		return e.writeErrRespPacket(conn, err)
	}

	for _, pkt := range respPackets {
		err = conn.WritePacket(pkt)
		if err != nil {
			return e.writeErrRespPacket(conn, err)
		}
	}
	return rows.Close()
}
