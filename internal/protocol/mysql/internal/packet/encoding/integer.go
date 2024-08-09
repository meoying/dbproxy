package encoding

import "encoding/binary"

// FixedLengthInteger 用于编码指定长度的整数
// byteSize的合法取值1,2,3,4,6,8
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_dt_integers.html#sect_protocol_basic_dt_int_fixed
func FixedLengthInteger(value uint64, byteSize int) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, value)
	return b[:byteSize]
}

// LengthEncodeInteger 对数字进行 int<lenenc> 编码
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_dt_integers.html#sect_protocol_basic_dt_int_le
func LengthEncodeInteger(value uint64) []byte {
	// 减少切片扩容按4+8容量去声明
	b := make([]byte, 0, 12)
	switch {
	case value < 0xFB:
		// [0, 251)	编码方式 1-byte integer
		b = append(b, byte(value))
	case value <= 0xFFFF:
		// [251, 2^16) 编码方式 0xFC + 2-byte integer
		b = append(b, 0xFC)
		b = append(b, FixedLengthInteger(value, 2)...)
	case value <= 0xFFFFFF:
		// [2^16, 2^24) 编码方式	0xFD + 3-byte integer
		b = append(b, 0xFD)
		b = append(b, FixedLengthInteger(value, 3)...)
	default:
		// [2^24, 2^64)	编码方式 0xFE + 8-byte integer
		b = append(b, 0xFE)
		b = append(b, FixedLengthInteger(value, 8)...)
	}
	return b
}
