package parser

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
)

type baseParser struct{}

// ParseLengthEncodedInteger 解析 Length-Encoded Integer
// 第二个返回值表述Integer使用n个字节来表示
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_dt_integers.html#sect_protocol_basic_dt_int_le
func (p *baseParser) ParseLengthEncodedInteger(buf *bytes.Buffer) (uint64, int, error) {
	firstByte, err := buf.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	switch {
	case firstByte < 0xFB:
		// [0, 251)	编码方式 1-byte integer
		return uint64(firstByte), 1, nil
	case firstByte == 0xFC:
		// [251, 2^16) 编码方式 0xFC + 2-byte integer
		var num uint16
		if err := binary.Read(buf, binary.LittleEndian, &num); err != nil {
			return 0, 0, err
		}
		return uint64(num), 2, nil
	case firstByte == 0xFD:
		// [2^16, 2^24) 编码方式	0xFD + 3-byte integer
		b := make([]byte, 3)
		if err := binary.Read(buf, binary.LittleEndian, b); err != nil {
			return 0, 0, err
		}
		var result uint64
		result |= uint64(b[0])
		result |= uint64(b[1]) << 8
		result |= uint64(b[2]) << 16
		return result, 3, nil
	case firstByte == 0xFE:
		var num uint64
		// [2^24, 2^64)	编码方式 0xFE + 8-byte integer
		if err := binary.Read(buf, binary.LittleEndian, &num); err != nil {
			return 0, 0, err
		}
		return num, 8, nil
	default:
		return 0, 0, fmt.Errorf("invalid length-encoded integer first byte: %d", firstByte)
	}
}

// ParseLengthEncodedString 解析 Length-Encoded String
func (p *baseParser) ParseLengthEncodedString(buf *bytes.Buffer) (string, error) {
	strLength, _, err := p.ParseLengthEncodedInteger(buf)
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

// ParseVariableLengthBinary 解析 Variable-Length Binary
func (p *baseParser) ParseVariableLengthBinary(buf *bytes.Buffer) ([]byte, error) {
	binLength, _, err := p.ParseLengthEncodedInteger(buf)
	if err != nil {
		return nil, err
	}

	binBytes := make([]byte, binLength)
	if _, err := buf.Read(binBytes); err != nil {
		return nil, err
	}

	return binBytes, nil
}
