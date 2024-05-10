package mysql

import (
	"encoding/binary"
	"testing"
)

// TestErrorPacketExample 这是一个 Error packet 的例子
// 方便你查看
func TestErrorPacketExample(t *testing.T) {
	data := []byte{0x17, 0x00, 0x00, 0x01, // 头部

		0xff,       // 标记是错误响应
		0x48, 0x04, // 错误码
		0x23,                         // sql state marker
		0x48, 0x59, 0x30, 0x30, 0x30, // sql state HY000
		// 错误信息
		0x4e, 0x6f, 0x20, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x73, 0x20, 0x75, 0x73, 0x65, 0x64}

	// 错误码  1096
	t.Log(uint16(data[5]) | uint16(data[6])<<8)
	t.Log(binary.LittleEndian.Uint16(data[5:7]))

	t.Log(string(data[8:13]))
	// no table used
	t.Log(string(data[13:]))
}
