package packet

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
