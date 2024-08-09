package builder

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/encoding"
)

type TextResultSetPacket struct {
	// capabilities 客户端与服务端建立连接时设置的flags
	capabilities flags.CapabilityFlags

	columnTypes  []ColumnType
	rows         [][]any
	serverStatus flags.SeverStatus
	charset      uint32

	MetadataFollows packet.ResultSetMetadata
	Error           error
}

func NewTextResultSetPacket(capabilities flags.CapabilityFlags, columnTypes []ColumnType, rows [][]any, serverStatus flags.SeverStatus, charset uint32) *TextResultSetPacket {
	return &TextResultSetPacket{capabilities: capabilities, columnTypes: columnTypes, rows: rows, serverStatus: serverStatus, charset: charset}
}

// Build 构建 text_resultset
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
func (b *TextResultSetPacket) Build() [][]byte {
	// resultset 由四种类型的包组成（字段数量包 + 字段描述包 + eof包 + 真实数据包）
	// 总包结构 = 字段数量包 + 字段数 * 字段描述包 + eof包 + 字段数 * 真实数据包 + eof包

	eofBuilder := EOFPacketBuilder{
		Capabilities: b.capabilities,
		StatusFlags:  b.serverStatus,
	}
	eofPacket := eofBuilder.Build()

	var packets [][]byte

	p := make([]byte, 4, 20)

	if b.capabilities.Has(flags.ClientOptionalResultsetMetadata) {
		// 	int<1>	metadata_follows	Flag specifying if metadata are skipped or not. See enum_resultset_metadata
		p = append(p, byte(b.MetadataFollows))
	}

	// int<lenenc>	column_count	Number of Column Definition to follow
	p = append(p, encoding.LengthEncodeInteger(uint64(len(b.columnTypes)))...)
	packets = append(packets, p)

	if !b.capabilities.Has(flags.ClientOptionalResultsetMetadata) ||
		b.MetadataFollows == packet.ResultSetMetadataFull {

		// column_count x Column Definition	Field metadata
		// one Column Definition for each field up to column_count
		for _, c := range b.columnTypes {
			packets = append(packets, b.buildColumnDefinitionPacket(c))
		}
	}

	if !b.capabilities.Has(flags.ClientDeprecateEOF) {
		if len(b.columnTypes) != 0 {
			// EOF_Packet	End of metadata	Marker to set the end of metadata
			packets = append(packets, eofPacket)
		}
	}

	// One or more Text Resultset Row
	// The row data	each Text Resultset Row contains column_count values
	rowBuilder := TextResultSetRowPacket{}
	for _, row := range b.rows {
		rowBuilder.values = row
		packets = append(packets, rowBuilder.Build())
	}

	if b.Error != nil {
		// ERR_Packet	terminator	Error details
		packets = append(packets, NewErrorPacketBuilder(b.capabilities, NewInternalError(b.Error)).Build())
	} else if b.capabilities.Has(flags.ClientDeprecateEOF) {
		// OK_Packet	terminator	All the execution details
		newEOF := OKOrEOFPacketBuilder{Capabilities: b.capabilities, StatusFlags: b.serverStatus}
		packets = append(packets, newEOF.BuildEOF())
	} else {
		// EOF_Packet	terminator	end of resultset marker
		packets = append(packets, eofPacket)
	}
	return packets
}

func (b *TextResultSetPacket) buildColumnDefinitionPacket(column ColumnType) []byte {
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
