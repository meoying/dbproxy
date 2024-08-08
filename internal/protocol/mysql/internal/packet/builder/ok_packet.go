package builder

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/encoding"
)

// OKOrEOFPacketBuilder OK/EOF包构建器
type OKOrEOFPacketBuilder struct {
	// Capabilities 客户端与服务端建立连接时设置的flags
	Capabilities flags.CapabilityFlags

	header byte
	// AffectedRows 仅OK包需要设置
	AffectedRows uint64
	// AffectedRows 仅OK包需要设置
	LastInsertID uint64
	// StatusFlags 客户端启用 ClientProtocol41 OK和EOF 需要设置此字段
	StatusFlags flags.SeverStatus
	// Warnings 客户端启用 ClientProtocol41 OK和EOF 需要设置此字段
	Warnings uint16
	// Info 仅OK包需要设置
	Info string
	// SessionStateInfo
}

// BuildOK 构造 OK_Packet
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html
func (b *OKOrEOFPacketBuilder) BuildOK() []byte {
	b.header = 0x00
	return b.build()
}

// BuildEOF 构造 EOF_Packet
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html
func (b *OKOrEOFPacketBuilder) BuildEOF() []byte {
	/*
			These rules distinguish whether the packet represents OK or EOF:
				- OK: header = 0 and length of packet > 7
				- EOF: header = 0xfe and length of packet < 9
		          (https://mariadb.com/kb/en/0-packet/ Packet length is the length of the packet body.)
			To ensure backward compatibility between old (prior to 5.7.5) and new (5.7.5 and up) versions of MySQL,
			new clients advertise the CLIENT_DEPRECATE_EOF flag:
				- Old clients do not know about this flag and do not advertise it.
			      Consequently, the server does not send OK packets that represent EOF.
			      (Old servers never do this, anyway. New servers recognize the absence of the flag to mean they should not.)
				- New clients advertise this flag. Old servers do not know this flag and do not send OK packets that represent EOF.
			      New servers recognize the flag and can send OK packets that represent EOF.
	*/
	b.header = 0xFE
	return b.build()
}

func (b *OKOrEOFPacketBuilder) build() []byte {
	// 头部的四个字节保留，不需要填充
	p := make([]byte, 4, 11)

	// int<1>  header 0x00 表示OK,0xFE 表示EOF
	p = append(p, b.header)

	// int<lenenc>	affected_rows 受影响的行数
	p = append(p, encoding.LengthEncodeInteger(b.AffectedRows)...)

	// int<lenenc>	last_insert_id 最后插入的ID
	p = append(p, encoding.LengthEncodeInteger(b.LastInsertID)...)

	// if capabilities & CLIENT_PROTOCOL_41 {
	if b.Capabilities.Has(flags.ClientProtocol41) {

		// int<2>	status_flags	SERVER_STATUS_flags_enum 服务器状态
		p = append(p, encoding.FixedLengthInteger(uint64(b.StatusFlags), 2)...)

		// int<2>	warnings 警告数
		p = append(p, encoding.FixedLengthInteger(uint64(b.Warnings), 2)...)

	} else if b.Capabilities.Has(flags.ClientTransactions) {

		// int<2>	status_flags	SERVER_STATUS_flags_enum 服务器状态
		p = append(p, encoding.FixedLengthInteger(uint64(b.StatusFlags), 2)...)
	}

	if b.Capabilities.Has(flags.ClientSessionTrack) {
		// string<lenenc>	info	human-readable status information
		p = append(p, encoding.LengthEncodeString(b.Info)...)

		if b.StatusFlags.Has(flags.ServerSessionStateChanged) {
			// string<lenenc>	session state info	Session State Information
			panic("暂不支持客户端设置 ClientSessionTrack ")
		}
	} else {
		// string<EOF>	info human-readable status information
		p = append(p, b.Info...)
	}

	return p
}
