package builder

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/encoding"
)

type BinaryResultsetPacket struct {
	// capabilities 客户端与服务端建立连接时设置的flags
	capabilities flags.CapabilityFlags

	columnTypes  []ColumnType
	rows         [][]any
	serverStatus flags.SeverStatus
	charset      uint32
}

func NewBinaryResultsetPacket(capabilities flags.CapabilityFlags, columnTypes []ColumnType, rows [][]any, serverStatus flags.SeverStatus, charset uint32) *BinaryResultsetPacket {
	return &BinaryResultsetPacket{capabilities: capabilities, columnTypes: columnTypes, rows: rows, serverStatus: serverStatus, charset: charset}
}

// Build
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html
func (b *BinaryResultsetPacket) Build() ([][]byte, error) {
	// resultset 由四种类型的包组成（字段数量包 + 字段描述包 + eof包 + 真实数据包）
	// 总包结构 = 字段数量包 + 字段数 * 字段描述包 + eof包 + 字段数 * 真实数据包 + eof包

	eofPacket := NewEOFPacket(b.capabilities, b.serverStatus).Build()

	var packets [][]byte

	p := make([]byte, 4, 20)

	// int<lenenc>	column_count	Number of Column Definition to follow
	p = append(p, encoding.LengthEncodeInteger(uint64(len(b.columnTypes)))...)
	packets = append(packets, p)

	// column_count x Column Definition	Field metadata
	// one Column Definition for each field up to column_count
	for _, c := range b.columnTypes {
		packets = append(packets, b.buildColumnDefinitionPacket(c))
	}
	if len(b.columnTypes) != 0 {
		// EOF_Packet	End of metadata	Marker to set the end of metadata
		packets = append(packets, eofPacket)
	}

	// None or many Binary Protocol Resultset Row
	rowBuilder := BinaryResultsetRowPacket{}
	for _, row := range b.rows {
		rowBuilder.values = row
		rowBuilder.cols = b.columnTypes
		build, err := rowBuilder.Build()
		if err != nil {
			return nil, err
		}
		packets = append(packets, build)
	}

	// EOF_Packet	terminator	end of resultset marker
	packets = append(packets, eofPacket)

	return packets, nil
}

func (b *BinaryResultsetPacket) buildColumnDefinitionPacket(column ColumnType) []byte {
	bb := ColumnDefinition41Packet{
		Catalog:      "def",
		Schema:       "unsupported",
		Table:        "unsupported",
		OrgTable:     "unsupported",
		Name:         column.Name(),
		OrgName:      column.Name(),
		CharacterSet: b.charset,
		ColumnLength: packet.GetMysqlTypeMaxLength(column.DatabaseTypeName()),
		Type:         byte(packet.GetMySQLType(column.DatabaseTypeName())),
		Flags:        0,
		Decimals:     0,
	}
	return bb.Build()
}
