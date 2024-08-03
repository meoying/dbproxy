package packet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
)

// PrepareStmtRequestParser 用于解析客户端发送的 COM_STMT_PREPARE 包
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html
type PrepareStmtRequestParser struct {
	// int<1>	command	0x16: COM_STMT_PREPARE
	command byte
	// string<EOF>	query	The query to prepare
	query string
}

func NewPrepareStmtRequestParser() *PrepareStmtRequestParser {
	return &PrepareStmtRequestParser{}
}

func (p *PrepareStmtRequestParser) Parse(payload []byte) error {
	if len(payload) < 1 {
		return fmt.Errorf("请求格式非法: PrepareStmt")
	}
	if payload[0] != 0x16 {
		return fmt.Errorf("命令非法: %d", payload[0])
	}
	p.command = payload[0]
	p.query = string(payload[1:])
	return nil
}

func (p *PrepareStmtRequestParser) Command() byte {
	return p.command
}

func (p *PrepareStmtRequestParser) Query() string {
	return p.query
}

// PrepareStmtResponseBuilder 用于构建客户端发送的请求包 COM_STMT_PREPARE 的响应包
// BuildOk 用于构建 COM_STMT_PREPARE_OK 包
// BuildErr 用于构建 ERR_Packet 包
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html#sect_protocol_com_stmt_prepare_response_ok
type PrepareStmtResponseBuilder struct {

	// ClientCapabilityFlags 客户端与服务端建立连接时设置的flags
	ClientCapabilityFlags flags.CapabilityFlags

	// Charset 客户端与服务端建立连接时设置的
	Charset uint32

	// ServerStatus 服务端状态
	ServerStatus SeverStatus

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
	FieldMetadataFollows ResultSetMetadata
}

func (b *PrepareStmtResponseBuilder) BuildOK() [][]byte {

	var packets [][]byte

	packets = append(packets, b.buildFirstPacket())

	packets = append(packets, b.buildParameterDefinitionPackets()...)

	packets = append(packets, b.buildColumnDefinitionPackets()...)

	return packets
}

func (b *PrepareStmtResponseBuilder) buildFirstPacket() []byte {
	p := make([]byte, 4, 20)

	p = append(p, b.FieldStatus)

	p = append(p, FixedLengthInteger(b.FieldStatementID, 4)...)

	p = append(p, FixedLengthInteger(uint32(b.FieldNumColumns), 2)...)

	p = append(p, FixedLengthInteger(uint32(b.FieldNumParams), 2)...)

	p = append(p, b.FieldReserved)

	if len(p) > 12 {

		p = append(p, FixedLengthInteger(uint32(b.FieldWarningCount), 2)...)

		if b.isClientOptionalResultsetMetadataFlagSet() {
			p = append(p, byte(b.FieldMetadataFollows))
		}
	}
	return p
}

func (b *PrepareStmtResponseBuilder) isClientOptionalResultsetMetadataFlagSet() bool {
	return b.ClientCapabilityFlags.Has(flags.CLIENT_OPTIONAL_RESULTSET_METADATA)
}

func (b *PrepareStmtResponseBuilder) buildParameterDefinitionPackets() [][]byte {
	if b.FieldNumParams > 0 && !b.isClientOptionalResultsetMetadataFlagSet() || b.FieldMetadataFollows == RESULTSET_METADATA_FULL {

		params := make([]Column, 0, b.FieldNumParams)
		for i := uint16(0); i < b.FieldNumParams; i++ {
			// 伪造参数定义
			params = append(params, NewColumn("?", "BIGINT"))
		}

		var packets [][]byte
		for _, p := range params {
			packets = append(packets, BuildColumnDefinitionPacket(p, b.Charset))
		}

		if !b.isClientDeprecateEOFFlagSet() {
			// 发送EOF包
			packets = append(packets, BuildEOFPacket(b.ServerStatus))
		} else {
			// 发送ok包 表示 中间的EOF
			// append(packets, EOF)
			panic("TODO: 用OK包表示EOF")
		}
		return packets
	}
	return nil
}

func (b *PrepareStmtResponseBuilder) isClientDeprecateEOFFlagSet() bool {
	return b.ClientCapabilityFlags.Has(flags.CLIENT_DEPRECATE_EOF)
}

func (b *PrepareStmtResponseBuilder) buildColumnDefinitionPackets() [][]byte {
	if b.FieldNumColumns > 0 && !b.isClientOptionalResultsetMetadataFlagSet() || b.FieldMetadataFollows == RESULTSET_METADATA_FULL {

		fields := make([]Column, 0, b.FieldNumColumns)
		for i := uint16(0); i < b.FieldNumColumns; i++ {
			fields = append(fields, NewColumn(fmt.Sprintf("fake_field_%d", i), "INT"))
		}

		var packets [][]byte
		for _, f := range fields {
			packets = append(packets, BuildColumnDefinitionPacket(f, b.Charset))
		}

		if !b.isClientDeprecateEOFFlagSet() {
			// 发送EOF包
			packets = append(packets, BuildEOFPacket(b.ServerStatus))
		} else {
			// 发送ok包 表示 中间的EOF
			panic("TODO: 用OK包表示EOF")
		}
		return packets
	}
	return nil
}

// ExecuteStmtRequestParser 用于解析客户端发送的 COM_STMT_EXECUTE 包
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html
type ExecuteStmtRequestParser struct {
	*baseParser

	// clientCapabilityFlags 客户端与服务端建立连接、握手时传递的参数,从 connection.Conn 中获取
	// 不属于 COM_STMT_EXECUTE 包
	clientCapabilityFlags flags.CapabilityFlags

	// numParams 之前传递的 prepare 语句中参数的个数
	numParams uint64

	// 以下是 COM_STMT_EXECUTE 包的各个字段

	// int<1>	status 默认 [0x17] 表示 COM_STMT_EXECUTE
	status byte

	// int<4>	statement_id	ID of the prepared statement to execute
	statementID uint32

	// int<1>	flags 相见 CursorType
	flags byte

	// int<4>	iteration_count	Number of times to execute the statement.
	// Currently, always 1.
	iterationCount uint32

	// int<lenenc>	parameter_count	The number of parameter metadata and values supplied.
	// Overrides the count coming from prepare (num_params) if present.
	// parameterCount 当 ClientQueryAttributes 设置才会解析
	parameterCount uint64

	// binary<var>	null_bitmap	NULL bitmap, length= (parameter_count + 7) / 8
	// 当 parameterCount > 0 才会解析
	nullBitmap []byte

	// int<1>	new_params_bind_flag	Flag if parameters must be re-bound
	// 当 parameterCount > 0 才会解析
	newParamsBindFlag byte

	// binary<var>	parameter_values	value of each parameter
	// parameters 当 newParamsBindFlag != 0 才会解析
	parameters []ExecuteStmtRequestParameter
}

type ExecuteStmtRequestParameter struct {
	// Type 当 NewParamsBindFlag != 0 才会解析
	// 第一个字节表示类型, 第二个字节表示是有符号还是无符号
	Type MySQLType
	// Name 当 newParamsBindFlag != 0 && ClientQueryAttributes 设置 才会解析
	Name  string
	Value any
}

func NewExecuteStmtRequestParser(clientCapabilityFlags flags.CapabilityFlags, numParams uint64) *ExecuteStmtRequestParser {
	return &ExecuteStmtRequestParser{
		baseParser:            &baseParser{},
		clientCapabilityFlags: clientCapabilityFlags,
		numParams:             numParams,
	}
}

func (p *ExecuteStmtRequestParser) Parse(payload []byte) error {
	log.Printf("pay = %#v\n", payload)
	buf := bytes.NewBuffer(payload)

	// 解析 Command
	if err := binary.Read(buf, binary.LittleEndian, &p.status); err != nil {
		return fmt.Errorf("error reading Command: %v", err)
	}

	// Command 验证
	if p.status != 0x17 {
		return fmt.Errorf("invalid Command: %x", p.status)
	}

	// 解析 StatementID
	if err := binary.Read(buf, binary.LittleEndian, &p.statementID); err != nil {
		return fmt.Errorf("error reading StatementID: %v", err)
	}

	// 解析 Flags
	if err := binary.Read(buf, binary.LittleEndian, &p.flags); err != nil {
		return fmt.Errorf("error reading Flags: %v", err)
	}

	// 解析 IterationCount
	if err := binary.Read(buf, binary.LittleEndian, &p.iterationCount); err != nil {
		return fmt.Errorf("error reading IterationCount: %v", err)
	}

	// 判断并解析参数数量
	var err error

	if p.numParams > 0 || (p.isClientQueryAttributesFlagOn() && (CursorType(p.flags)&PARAMETER_COUNT_AVAILABLE) != 0) {

		if p.isClientQueryAttributesFlagOn() {
			log.Printf("isClientQueryAttributesFlagOn = %#v\n", p.clientCapabilityFlags)
			p.parameterCount, err = p.ParseLengthEncodedInteger(buf)
			if err != nil {
				return fmt.Errorf("error reading ParameterCount: %v", err)
			}
		} else {
			p.parameterCount = p.numParams
		}

		// 如果 ParameterCount 大于 0
		if p.parameterCount > 0 {

			nullBitmapLen := (p.parameterCount + 7) / 8
			p.nullBitmap = make([]byte, nullBitmapLen)
			if _, err := buf.Read(p.nullBitmap); err != nil {
				return fmt.Errorf("error reading NullBitmap: %v", err)
			}

			log.Printf("req.nullBitmap = %#v\n", p.nullBitmap)

			log.Printf("buf = %#v\n", buf.Bytes())
			if err := binary.Read(buf, binary.LittleEndian, &p.newParamsBindFlag); err != nil {
				return fmt.Errorf("error reading NewParamsBindFlag: %v", err)
			}

			log.Printf("NewParamsBindFlag = %#v\n", p.newParamsBindFlag)

			err1 := p.parseParameters(buf)
			if err1 != nil {
				return err1
			}
		}
	}

	return nil
}

func (p *ExecuteStmtRequestParser) isClientQueryAttributesFlagOn() bool {
	return p.clientCapabilityFlags.Has(flags.ClientQueryAttributes)
}

func (p *ExecuteStmtRequestParser) isNewParamsBindFlagOn() bool {
	return p.newParamsBindFlag != 0
}

func (p *ExecuteStmtRequestParser) parseParameters(buf *bytes.Buffer) error {

	p.parameters = make([]ExecuteStmtRequestParameter, p.parameterCount)

	err2 := p.parseParametersType(buf)
	if err2 != nil {
		return err2
	}

	err3 := p.parseParametersName(buf)
	if err3 != nil {
		return err3
	}

	err4 := p.parseParametersValue(buf)
	if err4 != nil {
		return err4
	}
	return nil
}

func (p *ExecuteStmtRequestParser) parseParametersType(buf *bytes.Buffer) error {
	if p.isNewParamsBindFlagOn() {
		for i := uint64(0); i < p.parameterCount; i++ {
			if err := binary.Read(buf, binary.LittleEndian, &p.parameters[i].Type); err != nil {
				return fmt.Errorf("解析参数[%d]的类型失败: %v", i, err)
			}
		}
	}
	return nil
}

func (p *ExecuteStmtRequestParser) parseParametersName(buf *bytes.Buffer) error {
	if p.isNewParamsBindFlagOn() && p.isClientQueryAttributesFlagOn() {
		for i := uint64(0); i < p.parameterCount; i++ {
			name, err := p.ParseLengthEncodedString(buf)
			if err != nil {
				return fmt.Errorf("解析参数[%d]的名称失败: %v", i, err)
			}
			p.parameters[i].Name = name
		}
	}
	return nil
}

func (p *ExecuteStmtRequestParser) parseParametersValue(buf *bytes.Buffer) error {
	for i := uint64(0); i < p.parameterCount; i++ {
		value, err := p.parseParameterValue(buf, p.parameters[i].Type)
		if err != nil {
			return fmt.Errorf("解析参数[%d]的数值失败: %v", i, err)
		}
		p.parameters[i].Value = value
	}
	return nil
}

// parseParameterValue 根据字段的类型来读取对应的字段值
func (p *ExecuteStmtRequestParser) parseParameterValue(buf *bytes.Buffer, fieldType MySQLType) (any, error) {
	switch fieldType {
	case MySQLTypeLongLong:
		var value int64
		if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
			return nil, fmt.Errorf("error reading LONGLONG value: %v", err)
		}
		return value, nil
	case MySQLTypeLong:
		var value int32
		if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
			return nil, fmt.Errorf("error reading LONG value: %v", err)
		}
		return value, nil
	case MySQLTypeShort:
		var value int16
		if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
			return nil, fmt.Errorf("error reading SHORT value: %v", err)
		}
		return value, nil
	case MySQLTypeTiny:
		var value int8
		if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
			return nil, fmt.Errorf("error reading TINY value: %v", err)
		}
		return value, nil
	case MySQLTypeFloat:
		var value float32
		if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
			return nil, fmt.Errorf("error reading FLOAT value: %v", err)
		}
		return value, nil
	case MySQLTypeDouble:
		var value float64
		if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
			return nil, fmt.Errorf("error reading DOUBLE value: %v", err)
		}
		return value, nil
	case MySQLTypeString, MySQLTypeVarchar, MySQLTypeVarString, MySQLTypeDecimal:
		return p.ParseLengthEncodedString(buf)
	default:
		return nil, fmt.Errorf("支持的的参数类型: %d", fieldType)
	}
}

func (p *ExecuteStmtRequestParser) Parameters() []ExecuteStmtRequestParameter {
	return p.parameters
}
