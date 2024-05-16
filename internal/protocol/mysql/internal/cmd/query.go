package cmd

import (
	"github.com/meoying/dbproxy/internal/plugin"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/query"
)

var _ Executor = &QueryExecutor{}

type QueryExecutor struct {
	plugin plugin.Plugin
}

func NewQueryExecutor(plugin plugin.Plugin) *QueryExecutor {
	return &QueryExecutor{
		plugin: plugin,
	}
}

// Exec
// Query 命令的 payload 格式在
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html
func (exec *QueryExecutor) Exec(ctx *Context, payload []byte) error {
	que := exec.parseQuery(payload)
	// 在这里执行 que，并且写回响应
	result, err := exec.plugin.Handle(ctx, que.SQL)
	if err != nil {
		// 回写错误响应
		// 先返回系统错误
		errResp := packet.BuildErInternalError(err.Error())
		return ctx.Conn.WritePacket(packet.BuildErrRespPacket(errResp))
	}
	cols, err := result.Rows.Columns()
	if err != nil {
		errResp := packet.BuildErInternalError(err.Error())
		return ctx.Conn.WritePacket(packet.BuildErrRespPacket(errResp))
	}
	var data [][]any
	for result.Rows.Next() {
		row := make([]any, len(cols))
		err = result.Rows.Scan(row...)
		if err != nil {
			errResp := packet.BuildErInternalError(err.Error())
			return ctx.Conn.WritePacket(packet.BuildErrRespPacket(errResp))
		}
		data = append(data, row)
	}

	resp, err := exec.resp(cols, data)
	if err != nil {
		errResp := packet.BuildErInternalError(err.Error())
		return ctx.Conn.WritePacket(packet.BuildErrRespPacket(errResp))
	}
	for _, pkt := range resp {
		err = ctx.Conn.WritePacket(pkt)
		if err != nil {
			errResp := packet.BuildErInternalError(err.Error())
			return ctx.Conn.WritePacket(packet.BuildErrRespPacket(errResp))
		}
	}
	return nil
}

// resp 根据执行结果返回转换成对应的格式并返回
// response 的 text_resultset的格式在
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
func (exec *QueryExecutor) resp(cols []string, rows [][]any) ([][]byte, error) {
	// text_resultset 由四种类型的包组成（字段数量包 + 字段描述包 + eof包 + 真实数据包）
	// 总包结构 = 字段数量包 + 字段数 * 字段描述包 + eof包 + 字段数 * 真实数据包 + eof包
	var packetArr [][]byte

	// 写入字段数量
	packetArr = append(packetArr, packet.EncodeIntLenenc(uint64(len(cols))))
	// 写入字段描述包
	for _, c := range cols {
		packetArr = append(packetArr, packet.BuildColumnDefinitionPacket(c))
	}
	packetArr = append(packetArr, packet.BuildEOFPacket())

	// 写入真实每行数据
	for _, row := range rows {
		for _, v := range row {
			packetArr = append(packetArr, packet.BuildRowPacket(v))
		}
	}
	packetArr = append(packetArr, packet.BuildEOFPacket())

	return packetArr, nil
}

func (exec *QueryExecutor) parseQuery(payload []byte) query.Query {
	// 第一个字节是 cmd
	return query.Query{
		SQL: string(payload[1:]),
	}
}
