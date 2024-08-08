package builder

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/encoding"
)

// EOFPacketBuilder MySQL 5.7.5以前EOF包构建器
type EOFPacketBuilder struct {
	// Capabilities 客户端与服务端建立连接时设置的flags
	Capabilities flags.CapabilityFlags

	// Warnings 客户端启用 ClientProtocol41 需要设置此字段
	Warnings uint16

	// StatusFlags 客户端启用 ClientProtocol41 需要设置此字段
	StatusFlags flags.SeverStatus
}

// Build 构造 EOF_Packet
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_eof_packet.html
func (b *EOFPacketBuilder) Build() []byte {
	// 头部的四个字节保留，不需要填充
	p := make([]byte, 4, 9)

	// int<1>	header	0xFE EOF packet header
	p = append(p, 0xFE)

	if b.Capabilities.Has(flags.ClientProtocol41) {
		// int<2>	warnings 警告数
		p = append(p, encoding.FixedLengthInteger(uint64(b.Warnings), 2)...)

		// int<2>	status_flags	SERVER_STATUS_flags_enum 服务器状态
		p = append(p, encoding.FixedLengthInteger(uint64(b.StatusFlags), 2)...)
	}
	return p
}
