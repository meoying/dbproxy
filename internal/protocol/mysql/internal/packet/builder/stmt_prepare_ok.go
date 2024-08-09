package builder

import (
	"fmt"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/encoding"
)

// StmtPrepareOKPacket 用于构建客户端发送的请求包 COM_STMT_PREPARE 的响应包
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html#sect_protocol_com_stmt_prepare_response_ok
type StmtPrepareOKPacket struct {
	// capabilities 客户端与服务端建立连接时设置的flags
	capabilities flags.CapabilityFlags
	// serverStatus 服务端状态
	serverStatus flags.SeverStatus
	// charset 客户端与服务端建立连接时设置的
	charset uint32

	// 以下是 COM_STMT_PREPARE_OK 包内容
	// int<1>	status	0x00: OK: Ignored by cli_read_prepare_result
	Status byte

	// int<4>	statement_id	statement ID
	StatementID uint32

	// int<2>	num_columns	Number of columns
	NumColumns uint16

	// int<2>	num_params	Number of parameters
	NumParams uint16

	// int<1>	reserved_1	[00] filler
	// 保留字段,默认为0x00
	Reserved byte

	// 下列字段当 packet_length > 12 时 才会被写入

	// int<2>	warning_count	Number of warnings
	WarningCount uint16

	// int<1>	metadata_follows	Flag specifying if metadata are skipped or not.
	// 详见 resultset_metadata.go
	// 该字段当 CLIENT_OPTIONAL_RESULTSET_METADATA 设置时才会写入
	MetadataFollows packet.ResultSetMetadata
}

func NewStmtPrepareOKPacket(capabilities flags.CapabilityFlags, serverStatus flags.SeverStatus, charset uint32) *StmtPrepareOKPacket {
	return &StmtPrepareOKPacket{capabilities: capabilities, serverStatus: serverStatus, charset: charset}
}

func (b *StmtPrepareOKPacket) Build() [][]byte {

	var packets [][]byte

	packets = append(packets, b.buildFirstPacket())

	packets = append(packets, b.buildParameterDefinitionPackets()...)

	packets = append(packets, b.buildColumnDefinitionPackets()...)

	return packets
}

func (b *StmtPrepareOKPacket) buildFirstPacket() []byte {
	p := make([]byte, 4, 20)

	p = append(p, b.Status)

	p = append(p, encoding.FixedLengthInteger(uint64(b.StatementID), 4)...)

	p = append(p, encoding.FixedLengthInteger(uint64(b.NumColumns), 2)...)

	p = append(p, encoding.FixedLengthInteger(uint64(b.NumParams), 2)...)

	p = append(p, b.Reserved)

	if len(p) > 12 {

		p = append(p, encoding.FixedLengthInteger(uint64(b.WarningCount), 2)...)

		if b.isClientOptionalResultsetMetadataFlagSet() {
			p = append(p, byte(b.MetadataFollows))
		}
	}
	return p
}

func (b *StmtPrepareOKPacket) isClientOptionalResultsetMetadataFlagSet() bool {
	return b.capabilities.Has(flags.ClientOptionalResultsetMetadata)
}

func (b *StmtPrepareOKPacket) buildParameterDefinitionPackets() [][]byte {
	if b.NumParams > 0 && !b.isClientOptionalResultsetMetadataFlagSet() || b.MetadataFollows == packet.ResultSetMetadataFull {

		params := make([]Column, 0, b.NumParams)
		for i := uint16(0); i < b.NumParams; i++ {
			// 伪造参数定义
			params = append(params, NewColumn("?", "BIGINT"))
		}

		var packets [][]byte
		for _, p := range params {
			packets = append(packets, b.buildColumnDefinitionPacket(p))
		}

		packets = append(packets, b.buildEOFPacket())

		return packets
	}
	return nil
}

func (b *StmtPrepareOKPacket) buildEOFPacket() []byte {
	if b.isClientDeprecateEOFFlagSet() {
		// 发送ok包表示EOF
		return NewEOFProtocol41Packet(b.capabilities, b.serverStatus).Build()
	}
	// 发送EOF包
	return NewEOFPacket(b.capabilities, b.serverStatus).Build()
}

func (b *StmtPrepareOKPacket) isClientDeprecateEOFFlagSet() bool {
	return b.capabilities.Has(flags.ClientDeprecateEOF)
}

func (b *StmtPrepareOKPacket) buildColumnDefinitionPackets() [][]byte {
	if b.NumColumns > 0 && !b.isClientOptionalResultsetMetadataFlagSet() || b.MetadataFollows == packet.ResultSetMetadataFull {

		fields := make([]Column, 0, b.NumColumns)
		for i := uint16(0); i < b.NumColumns; i++ {
			fields = append(fields, NewColumn(fmt.Sprintf("fake_field_%d", i), "INT"))
		}

		var packets [][]byte
		for _, f := range fields {
			packets = append(packets, b.buildColumnDefinitionPacket(f))
		}

		packets = append(packets, b.buildEOFPacket())

		return packets
	}
	return nil
}

func (b *StmtPrepareOKPacket) buildColumnDefinitionPacket(column ColumnType) []byte {
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
