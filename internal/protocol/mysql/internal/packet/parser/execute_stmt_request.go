package parser

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
)

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
	Type packet.MySQLType
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

	if p.numParams > 0 || (p.isClientQueryAttributesFlagOn() && (packet.CursorType(p.flags)&packet.ParameterCountAvailable) != 0) {

		if p.isClientQueryAttributesFlagOn() {
			log.Printf("isClientQueryAttributesFlagOn = %#v\n", p.clientCapabilityFlags)
			p.parameterCount, _, err = p.ParseLengthEncodedInteger(buf)
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
func (p *ExecuteStmtRequestParser) parseParameterValue(buf *bytes.Buffer, fieldType packet.MySQLType) (any, error) {
	switch fieldType {
	case packet.MySQLTypeLongLong:
		var value int64
		if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
			return nil, fmt.Errorf("error reading LONGLONG value: %v", err)
		}
		return value, nil
	case packet.MySQLTypeLong:
		var value int32
		if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
			return nil, fmt.Errorf("error reading LONG value: %v", err)
		}
		return value, nil
	case packet.MySQLTypeShort:
		var value int16
		if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
			return nil, fmt.Errorf("error reading SHORT value: %v", err)
		}
		return value, nil
	case packet.MySQLTypeTiny:
		var value int8
		if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
			return nil, fmt.Errorf("error reading TINY value: %v", err)
		}
		return value, nil
	case packet.MySQLTypeFloat:
		var value float32
		if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
			return nil, fmt.Errorf("error reading FLOAT value: %v", err)
		}
		return value, nil
	case packet.MySQLTypeDouble:
		var value float64
		if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
			return nil, fmt.Errorf("error reading DOUBLE value: %v", err)
		}
		return value, nil
	case packet.MySQLTypeString, packet.MySQLTypeVarchar, packet.MySQLTypeVarString, packet.MySQLTypeDecimal:
		return p.ParseLengthEncodedString(buf)
	default:
		return nil, fmt.Errorf("支持的的参数类型: %d", fieldType)
	}
}

func (p *ExecuteStmtRequestParser) Parameters() []ExecuteStmtRequestParameter {
	return p.parameters
}
