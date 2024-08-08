package builder

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/encoding"
)

type BaseBuilder struct{}

// BuildTextResultsetRespPackets 根据执行结果返回转换成对应的格式并返回
// response 的 text_resultset 的格式在
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
func (p *BaseBuilder) BuildTextResultsetRespPackets(cols []packet.ColumnType, rows [][]any, serverStatus packet.SeverStatus, charset uint32) ([][]byte, error) {
	// text_resultset 由四种类型的包组成（字段数量包 + 字段描述包 + eof包 + 真实数据包）
	// 总包结构 = 字段数量包 + 字段数 * 字段描述包 + eof包 + 字段数 * 真实数据包 + eof
	return p.buildResultSetRespPackets(cols, rows, serverStatus, charset, packet.BuildTextResultsetRowRespPacket)
}

// BuildBinaryResultsetRespPackets 根据执行结果返回转换成对应的格式并返回
// response 的 binary_resultset 的格式在
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html
func (p *BaseBuilder) BuildBinaryResultsetRespPackets(cols []packet.ColumnType, rows [][]any, serverStatus packet.SeverStatus, charset uint32) ([][]byte, error) {
	// binary_resultset 由四种类型的包组成（字段数量包 + 字段描述包 + eof包 + 真实数据包）
	// 总包结构 = 字段数量包 + 字段数 * 字段描述包 + eof包 + 字段数 * 真实数据包 + eof包
	return p.buildResultSetRespPackets(cols, rows, serverStatus, charset, packet.BuildBinaryResultsetRowRespPacket)
}

type buildResultsetRowRespPacket func(values []any, cols []packet.ColumnType) ([]byte, error)

func (p *BaseBuilder) buildResultSetRespPackets(cols []packet.ColumnType, rows [][]any, serverStatus packet.SeverStatus, charset uint32, buildRowRespPacketFunc buildResultsetRowRespPacket) ([][]byte, error) {
	// resultset 由四种类型的包组成（字段数量包 + 字段描述包 + eof包 + 真实数据包）
	// 总包结构 = 字段数量包 + 字段数 * 字段描述包 + eof包 + 字段数 * 真实数据包 + eof包
	var packets [][]byte

	// 写入字段数量
	colLenPack := append([]byte{0, 0, 0, 0}, encoding.LengthEncodeInteger(uint64(len(cols)))...)
	packets = append(packets, colLenPack)

	// 写入字段描述包
	for _, c := range cols {
		packets = append(packets, packet.BuildColumnDefinitionPacket(c, charset))
	}
	if len(cols) != 0 {
		packets = append(packets, packet.BuildEOFPacket(serverStatus))
	}

	// 写入真实每行数据
	for _, row := range rows {
		pkt, err := buildRowRespPacketFunc(row, cols)
		if err != nil {
			return nil, err
		}
		packets = append(packets, pkt)
	}

	packets = append(packets, packet.BuildEOFPacket(serverStatus))

	return packets, nil
}

// func (p *BaseBuilder) BuildErrRespPacket(err error) []byte {
// 	return packet.BuildErrRespPacket(packet.NewInternalError(err.Error()))
// }

func (p *BaseBuilder) BuildOKRespPacket(serverStatus packet.SeverStatus, rowsAffected, lastInsertID uint64) []byte {
	return packet.BuildOKRespPacket(serverStatus, rowsAffected, lastInsertID)
}
