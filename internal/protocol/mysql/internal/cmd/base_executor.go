package cmd

import (
	"database/sql"

	"github.com/ecodeclub/ekit/slice"
	"github.com/ecodeclub/ekit/sqlx"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/connection"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

type BaseExecutor struct {
}

func (e *BaseExecutor) parseQuery(payload []byte) string {
	// 第一个字节是 cmd
	return string(payload[1:])
}

func (e *BaseExecutor) getServerStatus(conn *connection.Conn) packet.SeverStatus {
	status := packet.ServerStatusAutoCommit
	if conn.InTransaction() {
		status |= packet.SeverStatusInTrans
	}
	return status
}

func (e *BaseExecutor) writeOKRespPacket(conn *connection.Conn, status packet.SeverStatus, affectedRows, lastInsertID uint64) error {
	// TODO 如果是插入、更新、删除行为应该把影响行数和最后插入ID给传进去
	return conn.WritePacket(packet.BuildOKRespPacket(status, affectedRows, lastInsertID))
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

func (e *BaseExecutor) writeErrRespPacket(conn *connection.Conn, err error) error {
	return conn.WritePacket(packet.BuildErrRespPacket(packet.BuildErInternalError(err.Error())))
}

type buildResultSetRespPackets func(cols []packet.ColumnType, rows [][]any, charset uint32, status packet.SeverStatus) ([][]byte, error)

type handleRowsFunc func(rows sqlx.Rows, conn *connection.Conn, status packet.SeverStatus) error

func (e *BaseExecutor) handlePluginResult(result *plugin.Result, conn *connection.Conn, handleRowsFunc handleRowsFunc) error {
	// 重置conn的事务状态
	conn.SetInTransaction(result.InTransactionState)

	status := packet.ServerStatusAutoCommit
	if result.InTransactionState {
		status |= packet.SeverStatusInTrans
	}

	if result.Rows != nil {
		return handleRowsFunc(result.Rows, conn, status)
	}

	if result.Result != nil {
		return e.handleResult(result.Result, conn, status)
	}

	return e.writeOKRespPacket(conn, status, 0, 0)
}

func (e *BaseExecutor) handleQueryRows(rows sqlx.Rows, conn *connection.Conn, status packet.SeverStatus) error {
	return e.handleRows(rows, conn, status, e.buildTextResultsetRespPackets, false)
}

func (e *BaseExecutor) handleRows(rows sqlx.Rows, conn *connection.Conn, status packet.SeverStatus, buildPacketsFunc buildResultSetRespPackets, isBinaryProtocol bool) error {
	// if conn.InTransaction() {
	// 	status |= packet.SeverStatusInTrans
	// }
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

		// if isBinaryProtocol {
		// 	row, err = e.convert(row, cols)
		// 	if err != nil {
		// 		return e.writeErrRespPacket(conn, err)
		// 	}
		// }

		data = append(data, row)
	}
	columnTypes := slice.Map(cols, func(idx int, src *sql.ColumnType) packet.ColumnType {
		return src
	})
	packets, err := buildPacketsFunc(columnTypes, data, conn.CharacterSet(), status)
	if err != nil {
		return e.writeErrRespPacket(conn, err)
	}

	err = e.writeRespPackets(conn, packets)
	if err != nil {
		return err
	}
	return rows.Close()
}

// buildTextResultsetRespPackets 根据执行结果返回转换成对应的格式并返回
// response 的 text_resultset 的格式在
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
func (e *BaseExecutor) buildTextResultsetRespPackets(cols []packet.ColumnType, rows [][]any, charset uint32, status packet.SeverStatus) ([][]byte, error) {
	// text_resultset 由四种类型的包组成（字段数量包 + 字段描述包 + eof包 + 真实数据包）
	// 总包结构 = 字段数量包 + 字段数 * 字段描述包 + eof包 + 字段数 * 真实数据包 + eof
	return e.buildResultSetRespPackets(cols, rows, charset, status, packet.BuildTextResultsetRowRespPacket)
}

type buildResultsetRowRespPacket func(values []any, cols []packet.ColumnType) []byte

func (e *BaseExecutor) buildResultSetRespPackets(cols []packet.ColumnType, rows [][]any, charset uint32, status packet.SeverStatus, buildFunc buildResultsetRowRespPacket) ([][]byte, error) {
	// resultset 由四种类型的包组成（字段数量包 + 字段描述包 + eof包 + 真实数据包）
	// 总包结构 = 字段数量包 + 字段数 * 字段描述包 + eof包 + 字段数 * 真实数据包 + eof包
	var packets [][]byte

	// 写入字段数量
	colLenPack := append([]byte{0, 0, 0, 0}, packet.LengthEncodeInteger(uint64(len(cols)))...)
	packets = append(packets, colLenPack)
	// 写入字段描述包
	for _, c := range cols {
		packets = append(packets, packet.BuildColumnDefinitionPacket(c, charset))
	}
	packets = append(packets, packet.BuildEOFPacket(status))

	// 写入真实每行数据
	for _, row := range rows {
		packets = append(packets, buildFunc(row, cols))
	}
	packets = append(packets, packet.BuildEOFPacket(status))

	return packets, nil
}

func (e *BaseExecutor) handlePrepareRows(rows sqlx.Rows, conn *connection.Conn, status packet.SeverStatus) error {
	return e.handleRows(rows, conn, status, e.buildBinaryResultsetRespPackets, true)
}

// buildBinaryResultsetRespPackets 根据执行结果返回转换成对应的格式并返回
// response 的 binary_resultset 的格式在
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html
func (e *BaseExecutor) buildBinaryResultsetRespPackets(cols []packet.ColumnType, rows [][]any, charset uint32, status packet.SeverStatus) ([][]byte, error) {
	// binary_resultset 由四种类型的包组成（字段数量包 + 字段描述包 + eof包 + 真实数据包）
	// 总包结构 = 字段数量包 + 字段数 * 字段描述包 + eof包 + 字段数 * 真实数据包 + eof包
	return e.buildResultSetRespPackets(cols, rows, charset, status, packet.BuildBinaryResultsetRowRespPacket)
}

func (e *BaseExecutor) handleResult(result sql.Result, conn *connection.Conn, status packet.SeverStatus) error {
	rowsAffected, _ := result.RowsAffected()
	lastInsertId, _ := result.LastInsertId()
	return e.writeOKRespPacket(conn, status, uint64(rowsAffected), uint64(lastInsertId))
}
