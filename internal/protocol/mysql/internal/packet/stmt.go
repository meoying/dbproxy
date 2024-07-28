package packet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
)

type CapabilityFlag uint32

const (
	// ClientQueryAttributes
	// https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__capabilities__flags.html#ga3cd12e9fd3901274e239881796e5219b
	ClientQueryAttributes CapabilityFlag = 1 << 27
)

type CursorType byte

const (
	// ParameterCountAvailable  当客户端发送参数数量即使为0也开启该选项
	// https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#a3e5e9e744ff6f7b989a604fd669977da
	ParameterCountAvailable CursorType = 0x08
)

type ExecuteStmtRequest struct {
	Command        byte
	StatementID    uint32
	Flags          byte
	IterationCount uint32

	// ParameterCount 当ClientQueryAttributes is on 才会解
	ParameterCount uint64
	// NullBitmap 当 ParameterCount > 0 才会解析
	NullBitmap []byte
	// NewParamsBindFlag 当 ParameterCount > 0 才会解析
	NewParamsBindFlag byte
	// Parameters 当 NewParamsBindFlag != 0 才会解析
	Parameters []ExecuteStmtRequestParameter
}

type ExecuteStmtRequestParameter struct {
	// Type 当 NewParamsBindFlag != 0 才会解析
	// 第一个字节表示类型, 第二个字节表示是有符号还是无符号
	Type MySQLType
	// Name 当 NewParamsBindFlag != 0 && ClientQueryAttributes is on 才会解析
	Name  string
	Value any
}

// Parse 用于解析客户端发送的 COM_STMT_EXECUTE 包
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html
func (req *ExecuteStmtRequest) Parse(numParams uint64, payload []byte) error {
	log.Printf("pay = %#v\n", payload)
	buf := bytes.NewBuffer(payload)

	// 读取 Command
	if err := binary.Read(buf, binary.LittleEndian, &req.Command); err != nil {
		return fmt.Errorf("error reading Command: %v", err)
	}

	// Command 验证
	if req.Command != 0x17 {
		return fmt.Errorf("invalid Command: %x", req.Command)
	}

	// 读取 StatementID
	if err := binary.Read(buf, binary.LittleEndian, &req.StatementID); err != nil {
		return fmt.Errorf("error reading StatementID: %v", err)
	}

	// 读取 Flags
	if err := binary.Read(buf, binary.LittleEndian, &req.Flags); err != nil {
		return fmt.Errorf("error reading Flags: %v", err)
	}

	// 读取 IterationCount
	if err := binary.Read(buf, binary.LittleEndian, &req.IterationCount); err != nil {
		return fmt.Errorf("error reading IterationCount: %v", err)
	}

	// 判断并解析参数数量
	var err error

	isClientQueryAttributesFlagOn := (CapabilityFlag(req.Flags) & ClientQueryAttributes) != 0

	if numParams > 0 || (isClientQueryAttributesFlagOn && (CursorType(req.Flags)&ParameterCountAvailable) != 0) {

		if isClientQueryAttributesFlagOn {
			req.ParameterCount, err = readLengthEncodedInteger(buf)
			if err != nil {
				return fmt.Errorf("error reading ParameterCount: %v", err)
			}
		} else {
			req.ParameterCount = numParams
		}

		// 如果 ParameterCount 大于 0
		if req.ParameterCount > 0 {

			nullBitmapLen := (req.ParameterCount + 7) / 8
			req.NullBitmap = make([]byte, nullBitmapLen)
			if _, err := buf.Read(req.NullBitmap); err != nil {
				return fmt.Errorf("error reading NullBitmap: %v", err)
			}

			log.Printf("buf = %#v\n", buf.Bytes())
			if err := binary.Read(buf, binary.LittleEndian, &req.NewParamsBindFlag); err != nil {
				return fmt.Errorf("error reading NewParamsBindFlag: %v", err)
			}

			log.Printf("NewParamsBindFlag = %#v\n", req.NewParamsBindFlag)

			isNewParamsBindFlagOn := req.NewParamsBindFlag != 0
			req.Parameters = make([]ExecuteStmtRequestParameter, req.ParameterCount)

			for i := uint64(0); i < req.ParameterCount; i++ {

				if isNewParamsBindFlagOn {

					if err := binary.Read(buf, binary.LittleEndian, &req.Parameters[i].Type); err != nil {
						return fmt.Errorf("error reading ParameterType for param %d: %v", i, err)
					}

					if isClientQueryAttributesFlagOn {

						req.Parameters[i].Name, err = readLengthEncodedString(buf)
						if err != nil {
							return fmt.Errorf("error reading ParameterName for param %d: %v", i, err)
						}
					}
				}

				value, err := readParameterValue(buf, req.Parameters[i].Type)
				if err != nil {
					return fmt.Errorf("error reading ParameterValue for param %d: %v", i, err)
				}
				req.Parameters[i].Value = value
			}
		}
	}

	return nil
}

// readParameterValue 根据字段的类型来读取对应的字段值
func readParameterValue(buf *bytes.Buffer, fieldType MySQLType) (any, error) {
	switch fieldType {
	case MySQLTypeLongLong:
		var value uint64
		if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
			return nil, fmt.Errorf("error reading LONGLONG value: %v", err)
		}
		return value, nil
	case MySQLTypeLong:
		var value uint32
		if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
			return nil, fmt.Errorf("error reading LONG value: %v", err)
		}
		return value, nil
	case MySQLTypeShort:
		var value uint16
		if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
			return nil, fmt.Errorf("error reading SHORT value: %v", err)
		}
		return value, nil
	case MySQLTypeTiny:
		var value uint8
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
	case MySQLTypeString, MySQLTypeVarchar, MySQLTypeVarString:
		return readLengthEncodedString(buf)
	default:
		return nil, fmt.Errorf("unsupported parameter type %d", fieldType)
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
