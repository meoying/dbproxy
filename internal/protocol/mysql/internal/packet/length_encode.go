package packet

import "encoding/binary"

// ReadEncodedLength 读取编码长度
// 第二个返回值是长度用了几个字节
func ReadEncodedLength(b []byte) (uint64, int) {
	// See issue #349
	if len(b) == 0 {
		return 0, 1
	}

	switch b[0] {
	// 251: NULL
	case 0xfb:
		return 0, 1
	// 252: 后续两个字节
	case 0xfc:
		return uint64(b[1]) | uint64(b[2])<<8, 3

	// 253: 后续三个字节
	case 0xfd:
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16, 4

	// 254: 后续八个字节
	case 0xfe:
		// 下面注释描述的情况没有处理，目前还没遇到
		// If the first byte of a packet is a length-encoded integer and its byte value is 0xFE, you must check the length of the packet to verify that it has enough space for a 8-byte integer. If not, it may be an EOF_Packet instead.
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 |
			uint64(b[4])<<24 | uint64(b[5])<<32 | uint64(b[6])<<40 |
			uint64(b[7])<<48 | uint64(b[8])<<56, 9
	}

	// 0-250: 第一个字节就是数字
	return uint64(b[0]), 1
}

// LengthEncodeString 对字符串进行 string<lenenc> 编码
func LengthEncodeString(str string) []byte {
	// 将字符串的长度以 int<lenenc> 编码形式作为前缀与字符串内容拼接
	return append(LengthEncodeInteger(uint64(len(str))), []byte(str)...)
}

// LengthEncodeInteger 对数字进行 int<lenenc> 编码
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_dt_integers.html#sect_protocol_basic_dt_int_le
func LengthEncodeInteger(value uint64) []byte {
	// 减少切片扩容按4+8容量去声明
	encodedValue := make([]byte, 0, 12)

	switch {
	case value < 0xFB:
		// [0, 251)	编码方式 1-byte integer
		encodedValue = append(encodedValue, byte(value))
	case value <= 0xFFFF:
		// [251, 2^16) 编码方式 0xFC + 2-byte integer
		encodedValue = append(encodedValue, 0xFC)
		encodedValue = append(encodedValue, uint16ToBytes(uint16(value))...)
	case value <= 0xFFFFFF:
		// [2^16, 2^24) 编码方式	0xFD + 3-byte integer
		encodedValue = append(encodedValue, 0xFD)
		encodedValue = append(encodedValue, uint24ToBytes(uint32(value))...)
	default:
		// [2^24, 2^64)	编码方式 0xFE + 8-byte integer
		encodedValue = append(encodedValue, 0xFE)
		encodedValue = append(encodedValue, uint64ToBytes(value)...)
	}

	return encodedValue
}

// uint16ToBytes 将 uint16 转换为 2 字节
func uint16ToBytes(value uint16) []byte {
	bytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(bytes, value)
	return bytes
}

// uint24ToBytes 将 uint32 转换为 3 字节
func uint24ToBytes(value uint32) []byte {
	bytes := make([]byte, 3)
	binary.LittleEndian.PutUint32(bytes, value)
	return bytes
}

// uint64ToBytes 将 uint64 转换为 8 字节
func uint64ToBytes(value uint64) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, value)
	return bytes
}

// FixedLengthInteger 用于编码指定长度的整数
func FixedLengthInteger(value uint32, byteSize int) []byte {
	encodedValue := make([]byte, 4)
	binary.LittleEndian.PutUint32(encodedValue, value)
	return encodedValue[:byteSize]
}
