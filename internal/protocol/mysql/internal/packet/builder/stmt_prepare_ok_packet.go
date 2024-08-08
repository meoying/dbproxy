package builder

import (
	"fmt"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/encoding"
)

// StmtPrepareOKPacketBuilder 用于构建客户端发送的请求包 COM_STMT_PREPARE 的响应包
// BuildOk 用于构建 COM_STMT_PREPARE_OK 包
// BuildErr 用于构建 ERR_Packet 包
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html#sect_protocol_com_stmt_prepare_response_ok
type StmtPrepareOKPacketBuilder struct {
	*BaseBuilder

	// ClientCapabilityFlags 客户端与服务端建立连接时设置的flags
	ClientCapabilityFlags flags.CapabilityFlags

	// Charset 客户端与服务端建立连接时设置的
	Charset uint32

	// ServerStatus 服务端状态
	ServerStatus flags.SeverStatus

	// 以下是 COM_STMT_PREPARE_OK 包内容

	// int<1>	status	0x00: OK: Ignored by cli_read_prepare_result
	FieldStatus byte

	// int<4>	statement_id	statement ID
	FieldStatementID uint32

	// int<2>	num_columns	Number of columns
	FieldNumColumns uint16

	// int<2>	num_params	Number of parameters
	FieldNumParams uint16

	// int<1>	reserved_1	[00] filler
	// 保留字段,默认为0x00
	FieldReserved byte

	// 下列字段当 packet_length > 12 时 才会被写入

	// int<2>	warning_count	Number of warnings
	FieldWarningCount uint16

	// int<1>	metadata_follows	Flag specifying if metadata are skipped or not.
	// 详见 resultset_metadata.go
	// 该字段当 CLIENT_OPTIONAL_RESULTSET_METADATA 设置时才会写入
	FieldMetadataFollows packet.ResultSetMetadata
}

func (b *StmtPrepareOKPacketBuilder) Build() [][]byte {

	var packets [][]byte

	packets = append(packets, b.buildFirstPacket())

	packets = append(packets, b.buildParameterDefinitionPackets()...)

	packets = append(packets, b.buildColumnDefinitionPackets()...)

	return packets
}

func (b *StmtPrepareOKPacketBuilder) buildFirstPacket() []byte {
	p := make([]byte, 4, 20)

	p = append(p, b.FieldStatus)

	p = append(p, encoding.FixedLengthInteger(uint64(b.FieldStatementID), 4)...)

	p = append(p, encoding.FixedLengthInteger(uint64(b.FieldNumColumns), 2)...)

	p = append(p, encoding.FixedLengthInteger(uint64(b.FieldNumParams), 2)...)

	p = append(p, b.FieldReserved)

	if len(p) > 12 {

		p = append(p, encoding.FixedLengthInteger(uint64(b.FieldWarningCount), 2)...)

		if b.isClientOptionalResultsetMetadataFlagSet() {
			p = append(p, byte(b.FieldMetadataFollows))
		}
	}
	return p
}

func (b *StmtPrepareOKPacketBuilder) isClientOptionalResultsetMetadataFlagSet() bool {
	return b.ClientCapabilityFlags.Has(flags.ClientOptionalResultsetMetadata)
}

func (b *StmtPrepareOKPacketBuilder) buildParameterDefinitionPackets() [][]byte {
	if b.FieldNumParams > 0 && !b.isClientOptionalResultsetMetadataFlagSet() || b.FieldMetadataFollows == packet.ResultSetMetadataFull {

		params := make([]packet.Column, 0, b.FieldNumParams)
		for i := uint16(0); i < b.FieldNumParams; i++ {
			// 伪造参数定义
			params = append(params, packet.NewColumn("?", "BIGINT"))
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

func (b *StmtPrepareOKPacketBuilder) buildEOFPacket() []byte {
	if b.isClientDeprecateEOFFlagSet() {
		// 发送ok包表示EOF
		eofBuilder := OKOrEOFPacketBuilder{
			Capabilities: b.ClientCapabilityFlags,
			StatusFlags:  b.ServerStatus,
		}
		return eofBuilder.BuildEOF()
	}
	// 发送EOF包
	eofBuilder := EOFPacketBuilder{
		Capabilities: b.ClientCapabilityFlags,
		StatusFlags:  b.ServerStatus,
	}
	return eofBuilder.Build()
}

func (b *StmtPrepareOKPacketBuilder) isClientDeprecateEOFFlagSet() bool {
	return b.ClientCapabilityFlags.Has(flags.ClientDeprecateEOF)
}

func (b *StmtPrepareOKPacketBuilder) buildColumnDefinitionPackets() [][]byte {
	if b.FieldNumColumns > 0 && !b.isClientOptionalResultsetMetadataFlagSet() || b.FieldMetadataFollows == packet.ResultSetMetadataFull {

		fields := make([]packet.Column, 0, b.FieldNumColumns)
		for i := uint16(0); i < b.FieldNumColumns; i++ {
			fields = append(fields, packet.NewColumn(fmt.Sprintf("fake_field_%d", i), "INT"))
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

func (b *StmtPrepareOKPacketBuilder) buildColumnDefinitionPacket(column packet.ColumnType) []byte {
	bb := ColumnDefinition41Packet{
		Catalog:      "def",
		Schema:       "unsupported",
		Table:        "unsupported",
		OrgTable:     "unsupported",
		Name:         column.Name(),
		OrgName:      column.Name(),
		CharacterSet: b.Charset,
		ColumnLength: packet.GetMysqlTypeMaxLength(column.DatabaseTypeName()),
		Type:         byte(packet.GetMySQLType(column.DatabaseTypeName())),
		Flags:        0,
		Decimals:     0,
	}
	return bb.Build()
}
