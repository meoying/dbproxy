package cmd

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/connection"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
)

type baseExecutor struct {
}

func (e *baseExecutor) parseQuery(payload []byte) string {
	// 第一个字节是 cmd
	return string(payload[1:])
}

// parsePrepareStmtID stmtId 获取对应prepare ID
func (e *baseExecutor) parsePrepareStmtID(payload []byte) uint32 {
	var stmtId uint32
	// 第一个字节是 cmd
	reader := bytes.NewReader(payload[1:5])
	if err := binary.Read(reader, binary.LittleEndian, &stmtId); err != nil {
		return 0
	}
	return stmtId
}

// getPrepareStmtQuery 获取创建prepare的sql语句
func (e *baseExecutor) getPrepareStmtQuery(stmtId uint32, payload []byte) string {
	return fmt.Sprintf("PREPARE stmt%d FROM '%s'", stmtId, e.parseQuery(payload))
}

// getExecuteStmtQuery 获取执行prepare的sql语句
func (e *baseExecutor) getExecuteStmtQuery(stmtId uint32) string {
	return fmt.Sprintf("EXECUTE stmt%d", stmtId)
}

// getCloseStmtQuery 获取关闭prepare的sql语句
func (e *baseExecutor) getCloseStmtQuery(stmtId uint32) string {
	return fmt.Sprintf("DEALLOCATE PREPARE stmt%d", stmtId)
}

// buildTextResultSetRespPackets 根据执行结果返回转换成对应的格式并返回
// response 的 text_resultset 的格式在
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
func (e *baseExecutor) buildTextResultSetRespPackets(cols []packet.ColumnType, rows [][]any, charset uint32) ([][]byte, error) {
	// text_resultset 由四种类型的包组成（字段数量包 + 字段描述包 + eof包 + 真实数据包）
	// 总包结构 = 字段数量包 + 字段数 * 字段描述包 + eof包 + 字段数 * 真实数据包 + eof
	return e.buildResultSetRespPackets(cols, rows, charset, packet.BuildTextResultsetRowRespPacket)
}

type BuildResultsetRowRespPacket func(values ...any) []byte

func (e *baseExecutor) buildResultSetRespPackets(cols []packet.ColumnType, rows [][]any, charset uint32, buildFunc BuildResultsetRowRespPacket) ([][]byte, error) {
	// resultset 由四种类型的包组成（字段数量包 + 字段描述包 + eof包 + 真实数据包）
	// 总包结构 = 字段数量包 + 字段数 * 字段描述包 + eof包 + 字段数 * 真实数据包 + eof包
	var packets [][]byte

	// 写入字段数量
	colLenPack := append([]byte{0, 0, 0, 0}, packet.EncodeIntLenenc(uint64(len(cols)))...)
	packets = append(packets, colLenPack)
	// 写入字段描述包
	for _, c := range cols {
		packets = append(packets, packet.BuildColumnDefinitionPacket(c, charset))
	}
	packets = append(packets, packet.BuildEOFPacket())

	// 写入真实每行数据
	for _, row := range rows {
		packets = append(packets, buildFunc(row...))
	}
	packets = append(packets, packet.BuildEOFPacket())

	return packets, nil
}

// buildBinaryResultsetRespPackets 根据执行结果返回转换成对应的格式并返回
// response 的 binary_resultset 的格式在
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html
func (e *baseExecutor) buildBinaryResultsetRespPackets(cols []packet.ColumnType, rows [][]any, charset uint32) ([][]byte, error) {
	// binary_resultset 由四种类型的包组成（字段数量包 + 字段描述包 + eof包 + 真实数据包）
	// 总包结构 = 字段数量包 + 字段数 * 字段描述包 + eof包 + 字段数 * 真实数据包 + eof包
	return e.buildResultSetRespPackets(cols, rows, charset, packet.BuildBinaryResultsetRowRespPacket)
}

func (e *baseExecutor) writeOKRespPacket(conn *connection.Conn) error {
	// TODO 如果是插入、更新、删除行为应该把影响行数和最后插入ID给传进去
	return conn.WritePacket(packet.BuildOKResp(packet.ServerStatusAutoCommit))
}

func (e *baseExecutor) writeErrRespPacket(conn *connection.Conn, err error) error {
	return conn.WritePacket(packet.BuildErrRespPacket(packet.BuildErInternalError(err.Error())))
}
