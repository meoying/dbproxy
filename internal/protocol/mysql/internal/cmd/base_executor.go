package cmd

import (
	"database/sql"

	"github.com/ecodeclub/ekit/slice"
	"github.com/ecodeclub/ekit/sqlx"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/connection"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/builder"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

type BaseExecutor struct {
	// *builder.BaseBuilder
}

func (e *BaseExecutor) parseQuery(payload []byte) string {
	// 第一个字节是 cmd
	return string(payload[1:])
}

func (e *BaseExecutor) getServerStatus(conn *connection.Conn) flags.SeverStatus {
	status := flags.ServerStatusAutoCommit
	if conn.InTransaction() {
		status |= flags.SeverStatusInTrans
	}
	return status
}

func (e *BaseExecutor) writeOKRespPacket(conn *connection.Conn, status flags.SeverStatus, rowsAffected, lastInsertID uint64) error {
	b := builder.OKOrEOFPacketBuilder{
		Capabilities: conn.ClientCapabilityFlags(),
		AffectedRows: rowsAffected,
		LastInsertID: lastInsertID,
		StatusFlags:  status,
	}
	return conn.WritePacket(b.BuildOK())
}

func (e *BaseExecutor) writeErrRespPacket(conn *connection.Conn, err error) error {
	return conn.WritePacket(builder.NewErrorPacketBuilder(conn.ClientCapabilityFlags(), builder.NewInternalError(err)).Build())
}

func (e *BaseExecutor) writeRespPackets(conn *connection.Conn, packets [][]byte) error {
	for _, pkt := range packets {
		err := conn.WritePacket(pkt)
		if err != nil {
			return e.writeErrRespPacket(conn, err)
		}
	}
	return nil
}

// handleQuerySQLRows 处理使用非prepare语句获取到的结果集
func (e *BaseExecutor) handleQuerySQLRows(rows sqlx.Rows, conn *connection.Conn, status flags.SeverStatus) error {
	return e.handleRows(rows, conn, status, func(cols []builder.ColumnType, rows [][]any, serverStatus flags.SeverStatus, charset uint32) ([][]byte, error) {
		textResultSet := builder.NewTextResultSetPacket(conn.ClientCapabilityFlags(), cols, rows, serverStatus, charset)
		return textResultSet.Build(), nil
	})
}

// handlePrepareSQLRows 处理使用prepare语句获取到的结果集
func (e *BaseExecutor) handlePrepareSQLRows(rows sqlx.Rows, conn *connection.Conn, status flags.SeverStatus) error {
	return e.handleRows(rows, conn, status, func(cols []builder.ColumnType, rows [][]any, serverStatus flags.SeverStatus, charset uint32) ([][]byte, error) {
		binaryResultSet := builder.NewBinaryResultSetPacket(conn.ClientCapabilityFlags(), cols, rows, serverStatus, charset)
		return binaryResultSet.Build()
	})
}

type buildResultsetRespPacketsFunc func(cols []builder.ColumnType, rows [][]any, serverStatus flags.SeverStatus, charset uint32) ([][]byte, error)

func (e *BaseExecutor) handleRows(rows sqlx.Rows, conn *connection.Conn, status flags.SeverStatus, buildPacketsFunc buildResultsetRespPacketsFunc) error {
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

	columnTypes := slice.Map(cols, func(idx int, src *sql.ColumnType) builder.ColumnType {
		return src
	})

	packets, err := buildPacketsFunc(columnTypes, data, status, conn.CharacterSet())
	if err != nil {
		return e.writeErrRespPacket(conn, err)
	}

	err = e.writeRespPackets(conn, packets)
	if err != nil {
		return err
	}
	return rows.Close()
}

// handleSQLRowsFunc 对 handleQuerySQLRows 和 handlePrepareSQLRows 方法的抽象
type handleSQLRowsFunc func(rows sqlx.Rows, conn *connection.Conn, status flags.SeverStatus) error

// handlePluginResult 同一处理插件执行结果
func (e *BaseExecutor) handlePluginResult(result *plugin.Result, conn *connection.Conn, handleSQLRowsFunc handleSQLRowsFunc) error {
	// 重置conn的事务状态
	conn.SetInTransaction(result.InTransactionState)

	status := flags.ServerStatusAutoCommit
	if result.InTransactionState {
		status |= flags.SeverStatusInTrans
	}

	if result.Rows != nil {
		return handleSQLRowsFunc(result.Rows, conn, status)
	}

	if result.Result != nil {
		return e.handleSQLResult(result.Result, conn, status)
	}

	return e.writeOKRespPacket(conn, status, 0, 0)
}

func (e *BaseExecutor) handleSQLResult(result sql.Result, conn *connection.Conn, status flags.SeverStatus) error {
	rowsAffected, _ := result.RowsAffected()
	lastInsertId, _ := result.LastInsertId()
	return e.writeOKRespPacket(conn, status, uint64(rowsAffected), uint64(lastInsertId))
}
