package packet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
)

type baseParser struct{}

// ParseLengthEncodedInteger 解析 Length-Encoded Integer
func (p *baseParser) ParseLengthEncodedInteger(buf *bytes.Buffer) (uint64, error) {
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

// ParseLengthEncodedString 解析 Length-Encoded String
func (p *baseParser) ParseLengthEncodedString(buf *bytes.Buffer) (string, error) {
	strLength, err := p.ParseLengthEncodedInteger(buf)
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
	binLength, err := p.ParseLengthEncodedInteger(buf)
	if err != nil {
		return nil, err
	}

	binBytes := make([]byte, binLength)
	if _, err := buf.Read(binBytes); err != nil {
		return nil, err
	}

	return binBytes, nil
}
