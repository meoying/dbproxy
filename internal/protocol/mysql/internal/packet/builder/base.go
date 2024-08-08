package builder

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/encoding"
)

// const (
// 	// maxPacketSize 单一报文最大长度
// 	maxPacketSize = 1<<24 - 1
// )

type BaseBuilder struct{}

// BuildTextResultsetRespPackets 根据执行结果返回转换成对应的格式并返回
// response 的 text_resultset 的格式在
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
func (b *BaseBuilder) BuildTextResultsetRespPackets(cols []packet.ColumnType, rows [][]any, serverStatus flags.SeverStatus, charset uint32) ([][]byte, error) {
	// text_resultset 由四种类型的包组成（字段数量包 + 字段描述包 + eof包 + 真实数据包）
	// 总包结构 = 字段数量包 + 字段数 * 字段描述包 + eof包 + 字段数 * 真实数据包 + eof
	return b.buildResultSetRespPackets(cols, rows, serverStatus, charset, packet.BuildTextResultsetRowRespPacket)
}

// BuildBinaryResultsetRespPackets 根据执行结果返回转换成对应的格式并返回
// response 的 binary_resultset 的格式在
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html
func (b *BaseBuilder) BuildBinaryResultsetRespPackets(cols []packet.ColumnType, rows [][]any, serverStatus flags.SeverStatus, charset uint32) ([][]byte, error) {
	// binary_resultset 由四种类型的包组成（字段数量包 + 字段描述包 + eof包 + 真实数据包）
	// 总包结构 = 字段数量包 + 字段数 * 字段描述包 + eof包 + 字段数 * 真实数据包 + eof包
	return b.buildResultSetRespPackets(cols, rows, serverStatus, charset, packet.BuildBinaryResultsetRowRespPacket)
}

type buildResultsetRowRespPacket func(values []any, cols []packet.ColumnType) ([]byte, error)

func (b *BaseBuilder) buildResultSetRespPackets(cols []packet.ColumnType, rows [][]any, serverStatus flags.SeverStatus, charset uint32, buildRowRespPacketFunc buildResultsetRowRespPacket) ([][]byte, error) {
	// resultset 由四种类型的包组成（字段数量包 + 字段描述包 + eof包 + 真实数据包）
	// 总包结构 = 字段数量包 + 字段数 * 字段描述包 + eof包 + 字段数 * 真实数据包 + eof包
	var packets [][]byte

	eofBuilder := EOFPacketBuilder{
		Capabilities: flags.CapabilityFlags(flags.ClientProtocol41), // TODO: 需要改为从conn中获取
		StatusFlags:  serverStatus,
	}
	eofPacket := eofBuilder.Build()

	// 写入字段数量
	colLenPack := append([]byte{0, 0, 0, 0}, encoding.LengthEncodeInteger(uint64(len(cols)))...)
	packets = append(packets, colLenPack)

	// 写入字段描述包
	for _, c := range cols {
		packets = append(packets, packet.BuildColumnDefinitionPacket(c, charset))
	}
	if len(cols) != 0 {
		packets = append(packets, eofPacket)
	}

	// 写入真实每行数据
	for _, row := range rows {
		pkt, err := buildRowRespPacketFunc(row, cols)
		if err != nil {
			return nil, err
		}
		packets = append(packets, pkt)
	}

	packets = append(packets, eofPacket)

	return packets, nil
}

// func (b *BaseBuilder) buildPacket(data []byte) ([]byte, error) {
// 	pktLen := len(data) - 4
//
// 	if pktLen > maxPacketSize {
// 		return nil, fmt.Errorf("%w，最大长度 %d，报文长度 %d",
// 			errs.ErrPktTooLarge, maxPacketSize, pktLen)
// 	}
// 	// log.Printf("data[0] = %d, data[1] = %d, data[2] = %d, data[3] = %d\n", pktLen, pktLen>>8, pktLen>>16, mc.sequence)
// 	data[0] = byte(pktLen)
// 	data[1] = byte(pktLen >> 8)
// 	data[2] = byte(pktLen >> 16)
//
// 	// mc.sequence
// 	data[3] = 0x00
// 	return data, nil
// }
