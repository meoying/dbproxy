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
	clientCapabilityFlags flags.CapabilityFlags

	// 以下是 COM_STMT_PREPARE_OK 包内容

	// int<1>	status	0x00: OK: Ignored by cli_read_prepare_result
	Status byte

	// int<4>	statement_id	statement ID
	StatementID int32

	// int<2>	num_columns	Number of columns
	NumColumns int16

	// int<2>	num_params	Number of parameters
	NumParams int16

	// int<1>	reserved_1	[00] filler
	// 保留字段,默认为0x00
	reserved byte

	// 下列字段当 packet_length > 12 时 才会被写入

	// int<2>	warning_count	Number of warnings
	WarningCount int16

	// int<1>	metadata_follows	Flag specifying if metadata are skipped or not.
	// 详见 resultset_metadata.go
	// 该字段当 CLIENT_OPTIONAL_RESULTSET_METADATA 设置时才会写入
	MetadataFollows ResultSetMetadata
}

func NewPrepareStmtResponseBuilder(clientCapabilityFlags flags.CapabilityFlags) *PrepareStmtResponseBuilder {
	return &PrepareStmtResponseBuilder{
		clientCapabilityFlags: clientCapabilityFlags,
	}
}

func (b *PrepareStmtResponseBuilder) BuildOK() [][]byte {

	var packets [][]byte

	p := b.buildPayload()

	packets = append(packets, p)

	//
	i, done := b.buildParameterDefinitionBlock()
	if done {
		return i
	}

	if b.NumColumns > 0 && !b.isClientOptionalResultsetMetadataFlagSet() || b.MetadataFollows == RESULTSET_METADATA_FULL {

		// num_columns * Column Definition
		// num_columns * Column Definition
		if !b.isClientDeprecateEOFFlagSet() {
			// 发送EOF包
			return nil
		} else {
			// 发送ok包 表示 中间的EOF
			return nil
		}
	}

	return packets
}

func (b *PrepareStmtResponseBuilder) buildParameterDefinitionBlock() ([][]byte, bool) {
	if b.NumParams > 0 && !b.isClientOptionalResultsetMetadataFlagSet() || b.MetadataFollows == RESULTSET_METADATA_FULL {
		// Parameter definition block
		// num_params * Column Definition
		// num_params packets will follow
		if !b.isClientDeprecateEOFFlagSet() {
			// 发送EOF包
			return nil, true
		} else {
			// 发送ok包 表示 中间的EOF
			return nil, true
		}
	}
	return nil, false
}

func (b *PrepareStmtResponseBuilder) buildPayload() []byte {
	p := make([]byte, 4, 20)

	p = append(p, b.Status)

	p = append(p, FixedLengthInteger(uint32(b.StatementID), 4)...)

	p = append(p, FixedLengthInteger(uint32(b.NumColumns), 2)...)

	p = append(p, FixedLengthInteger(uint32(b.NumParams), 2)...)

	p = append(p, b.reserved)

	if len(p) > 12 {

		p = append(p, FixedLengthInteger(uint32(b.WarningCount), 2)...)

		if b.isClientOptionalResultsetMetadataFlagSet() {
			p = append(p, byte(b.MetadataFollows))
		}
	}
	return p
}

func (b *PrepareStmtResponseBuilder) isClientOptionalResultsetMetadataFlagSet() bool {
	return b.clientCapabilityFlags.Has(flags.CLIENT_OPTIONAL_RESULTSET_METADATA)
}

func (b *PrepareStmtResponseBuilder) isClientDeprecateEOFFlagSet() bool {
	return b.clientCapabilityFlags.Has(flags.CLIENT_DEPRECATE_EOF)
}

func (b *PrepareStmtResponseBuilder) BuildErr() []byte {
	return nil
}

// ExecuteStmtRequestParser 用于解析客户端发送的 COM_STMT_EXECUTE 包
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html
type ExecuteStmtRequestParser struct {

	// clientCapabilityFlags 客户端与服务端建立连接、握手时传递的参数,从 connection.Conn 中获取
	// 不属于 COM_STMT_EXECUTE 包
	clientCapabilityFlags flags.CapabilityFlags

	// numParams 之前传递的 prepare 语句中参数的个数
	numParams uint64

	// 一下是 COM_STMT_EXECUTE 包的各个字段

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
		clientCapabilityFlags: clientCapabilityFlags,
		numParams:             numParams,
	}
}

func (req *ExecuteStmtRequestParser) Parse(payload []byte) error {
	log.Printf("pay = %#v\n", payload)
	buf := bytes.NewBuffer(payload)

	// 解析 Command
	if err := binary.Read(buf, binary.LittleEndian, &req.status); err != nil {
		return fmt.Errorf("error reading Command: %v", err)
	}

	// Command 验证
	if req.status != 0x17 {
		return fmt.Errorf("invalid Command: %x", req.status)
	}

	// 解析 StatementID
	if err := binary.Read(buf, binary.LittleEndian, &req.statementID); err != nil {
		return fmt.Errorf("error reading StatementID: %v", err)
	}

	// 解析 Flags
	if err := binary.Read(buf, binary.LittleEndian, &req.flags); err != nil {
		return fmt.Errorf("error reading Flags: %v", err)
	}

	// 解析 IterationCount
	if err := binary.Read(buf, binary.LittleEndian, &req.iterationCount); err != nil {
		return fmt.Errorf("error reading IterationCount: %v", err)
	}

	// 判断并解析参数数量
	var err error

	if req.numParams > 0 || (req.isClientQueryAttributesFlagOn() && (CursorType(req.flags)&PARAMETER_COUNT_AVAILABLE) != 0) {

		if req.isClientQueryAttributesFlagOn() {
			log.Printf("isClientQueryAttributesFlagOn = %#v\n", req.clientCapabilityFlags)
			req.parameterCount, err = readLengthEncodedInteger(buf)
			if err != nil {
				return fmt.Errorf("error reading ParameterCount: %v", err)
			}
		} else {
			req.parameterCount = req.numParams
		}

		// 如果 ParameterCount 大于 0
		if req.parameterCount > 0 {

			nullBitmapLen := (req.parameterCount + 7) / 8
			req.nullBitmap = make([]byte, nullBitmapLen)
			if _, err := buf.Read(req.nullBitmap); err != nil {
				return fmt.Errorf("error reading NullBitmap: %v", err)
			}

			log.Printf("req.nullBitmap = %#v\n", req.nullBitmap)

			log.Printf("buf = %#v\n", buf.Bytes())
			if err := binary.Read(buf, binary.LittleEndian, &req.newParamsBindFlag); err != nil {
				return fmt.Errorf("error reading NewParamsBindFlag: %v", err)
			}

			log.Printf("NewParamsBindFlag = %#v\n", req.newParamsBindFlag)

			err1 := req.parseParameters(buf)
			if err1 != nil {
				return err1
			}
		}
	}

	return nil
}

func (req *ExecuteStmtRequestParser) isClientQueryAttributesFlagOn() bool {
	return req.clientCapabilityFlags.Has(flags.ClientQueryAttributes)
}

func (req *ExecuteStmtRequestParser) isNewParamsBindFlagOn() bool {
	return req.newParamsBindFlag != 0
}

func (req *ExecuteStmtRequestParser) parseParameters(buf *bytes.Buffer) error {

	req.parameters = make([]ExecuteStmtRequestParameter, req.parameterCount)

	err2 := req.parseParametersType(buf)
	if err2 != nil {
		return err2
	}

	err3 := req.parseParametersName(buf)
	if err3 != nil {
		return err3
	}

	err4 := req.parseParametersValue(buf)
	if err4 != nil {
		return err4
	}
	return nil
}

func (req *ExecuteStmtRequestParser) parseParametersType(buf *bytes.Buffer) error {
	if req.isNewParamsBindFlagOn() {
		for i := uint64(0); i < req.parameterCount; i++ {
			if err := binary.Read(buf, binary.LittleEndian, &req.parameters[i].Type); err != nil {
				return fmt.Errorf("解析参数[%d]的类型失败: %v", i, err)
			}
		}
	}
	return nil
}

func (req *ExecuteStmtRequestParser) parseParametersName(buf *bytes.Buffer) error {
	if req.isNewParamsBindFlagOn() && req.isClientQueryAttributesFlagOn() {
		for i := uint64(0); i < req.parameterCount; i++ {
			name, err := readLengthEncodedString(buf)
			if err != nil {
				return fmt.Errorf("解析参数[%d]的名称失败: %v", i, err)
			}
			req.parameters[i].Name = name
		}
	}
	return nil
}

func (req *ExecuteStmtRequestParser) parseParametersValue(buf *bytes.Buffer) error {
	for i := uint64(0); i < req.parameterCount; i++ {
		value, err := readParameterValue(buf, req.parameters[i].Type)
		if err != nil {
			return fmt.Errorf("解析参数[%d]的数值失败: %v", i, err)
		}
		req.parameters[i].Value = value
	}
	return nil
}

func (req *ExecuteStmtRequestParser) Parameters() []ExecuteStmtRequestParameter {
	return req.parameters
}

// readParameterValue 根据字段的类型来读取对应的字段值
func readParameterValue(buf *bytes.Buffer, fieldType MySQLType) (any, error) {
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
		return readLengthEncodedString(buf)
	default:
		return nil, fmt.Errorf("支持的的参数类型: %d", fieldType)
	}
}

// readLengthEncodedInteger 解析 Length-Encoded Integer
func readLengthEncodedInteger(buf *bytes.Buffer) (uint64, error) {
	firstByte, err := buf.ReadByte()
	if err != nil {
		return 0, err
	}

	switch {
	case firstByte < 0xfb:
		return uint64(firstByte), nil
	case firstByte == 0xfc:
		var num uint16
		if err := binary.Read(buf, binary.LittleEndian, &num); err != nil {
			return 0, err
		}
		return uint64(num), nil
	case firstByte == 0xfd:
		var num uint32
		if err := binary.Read(buf, binary.LittleEndian, &num); err != nil {
			return 0, err
		}
		return uint64(num & 0xFFFFFF), nil // 取24位
	case firstByte == 0xfe:
		var num uint64
		if err := binary.Read(buf, binary.LittleEndian, &num); err != nil {
			return 0, err
		}
		return num, nil
	default:
		return 0, fmt.Errorf("invalid length-encoded integer first byte: %d", firstByte)
	}
}

// readLengthEncodedString 解析 Length-Encoded String
func readLengthEncodedString(buf *bytes.Buffer) (string, error) {
	strLength, err := readLengthEncodedInteger(buf)
	if err != nil {
		return "", err
	}
	log.Printf("StrLength = %d\n", strLength)

	strBytes := make([]byte, strLength)
	if _, err := buf.Read(strBytes); err != nil {
		return "", err
	}
	log.Printf("StrBytes = %s\n", string(strBytes))
	return string(strBytes), nil
}

// readVariableLengthBinary 解析 Variable-Length Binary
// func readVariableLengthBinary(buf *bytes.Buffer) ([]byte, error) {
// 	binLength, err := readLengthEncodedInteger(buf)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	binBytes := make([]byte, binLength)
// 	if _, err := buf.Read(binBytes); err != nil {
// 		return nil, err
// 	}
//
// 	return binBytes, nil
// }
