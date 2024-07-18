package cmd

import (
	"context"
	"database/sql"

	"github.com/ecodeclub/ekit/sqlx"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/connection"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

var _ Executor = &QueryExecutor{}

type QueryExecutor struct {
	hdl plugin.Handler
}

func NewQueryExecutor(hdl plugin.Handler) *QueryExecutor {
	return &QueryExecutor{
		hdl: hdl,
	}
}

// Exec
// Query 命令的 payload 格式在
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html
func (exec *QueryExecutor) Exec(
	ctx context.Context,
	conn *connection.Conn,
	payload []byte) error {
	que := exec.parseQuery(payload)
	pctx := &pcontext.Context{
		Context: ctx,
		Query:   que,
		ParsedQuery: pcontext.ParsedQuery{
			Root: ast.Parse(que),
		},
		ConnID: conn.ID(),
	}

	// 在这里执行 que，并且写回响应
	result, err := exec.hdl.Handle(pctx)
	if err != nil {
		// 回写错误响应
		// 先返回系统错误
		errResp := packet.BuildErInternalError(err.Error())
		return conn.WritePacket(packet.BuildErrRespPacket(errResp))
	}

	// 重制conn的事务状态
	conn.SetInTransaction(result.TxInTransaction)

	if result.Rows != nil {
		return exec.handleRows(result.Rows, conn)
	}

	if result.TxInTransaction {
		return conn.WritePacket(packet.BuildOKResp(packet.SeverStatusInTrans | packet.ServerStatusAutoCommit))
	}

	// TODO 如果是插入、更新、删除行为应该把影响行数和最后插入ID给传进去
	return conn.WritePacket(packet.BuildOKResp(packet.ServerStatusAutoCommit))
}

func (exec *QueryExecutor) handleRows(rows sqlx.Rows, conn *connection.Conn) error {
	cols, err := rows.ColumnTypes()
	if err != nil {
		errResp := packet.BuildErInternalError(err.Error())
		return conn.WritePacket(packet.BuildErrRespPacket(errResp))
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
			errResp := packet.BuildErInternalError(err.Error())
			return conn.WritePacket(packet.BuildErrRespPacket(errResp))
		}
		data = append(data, row)
	}

	respPackets, err := exec.buildResultSetRespPackets(cols, data, conn.CharacterSet())
	if err != nil {
		errResp := packet.BuildErInternalError(err.Error())
		return conn.WritePacket(packet.BuildErrRespPacket(errResp))
	}

	for _, pkt := range respPackets {
		err = conn.WritePacket(pkt)
		if err != nil {
			errResp := packet.BuildErInternalError(err.Error())
			return conn.WritePacket(packet.BuildErrRespPacket(errResp))
		}
	}
	return rows.Close()
}

// buildResultSetRespPackets 根据执行结果返回转换成对应的格式并返回
// response 的 text_resultset的格式在
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
func (exec *QueryExecutor) buildResultSetRespPackets(cols []*sql.ColumnType, rows [][]any, charset uint32) ([][]byte, error) {
	// text_resultset 由四种类型的包组成（字段数量包 + 字段描述包 + eof包 + 真实数据包）
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
		packets = append(packets, packet.BuildRowPacket(row...))
	}
	packets = append(packets, packet.BuildEOFPacket())

	return packets, nil
}

func (exec *QueryExecutor) parseQuery(payload []byte) string {
	// 第一个字节是 cmd
	return string(payload[1:])
}
