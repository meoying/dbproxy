package builder

import (
	"encoding/binary"

	"github.com/ecodeclub/ekit/randx"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/encoding"
)

// HandshakeV10Packet 在 mysql 协议中，在建立了 TCP 连接之后
// mysql server 端发起 Handshake
// 而后客户端要响应 Handshake
// TODO 支持 SSL
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase.html#sect_protocol_connection_phase_initial_handshake
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
// 可以参考 README.md 中的一个例子
type HandshakeV10Packet struct {
	capabilities       flags.CapabilityFlags
	authPluginDataFunc func() string

	// 以下为协议包内可以用于设置的部分字段
	ProtocolVersion      byte
	ServerVersion        string
	ConnectionID         uint32
	CapabilityFlags1     uint16
	CharacterSet         byte
	StatusFlags          flags.SeverStatus
	CapabilityFlags2     uint16
	AuthPluginDataLength byte
	AuthPluginName       string
}

func NewHandshakeV10Packet(capabilities flags.CapabilityFlags, serverStatus flags.SeverStatus, AuthPluginDataGenerator func() string) *HandshakeV10Packet {
	return &HandshakeV10Packet{
		capabilities:         capabilities,
		StatusFlags:          serverStatus,
		CapabilityFlags1:     0xFFFF,
		CharacterSet:         0xFF,
		CapabilityFlags2:     0xDFFF,
		AuthPluginDataLength: 0x15,
		authPluginDataFunc:   AuthPluginDataGenerator,
	}
}

func (b *HandshakeV10Packet) Build() []byte {

	p := make([]byte, 4, 50)

	// int<1>	protocol version	Always 10
	// 设置协议版本
	// p[0] = b.ProtocolVersion
	p = append(p, b.ProtocolVersion)

	// string<NUL>	server version	human-readable status information
	// 这里我们将自己定义为是 8.4.0 的版本
	p = append(p, encoding.NullTerminatedString(b.ServerVersion)...)

	// 	int<4>	thread id	a.k.a. connection id
	p = binary.LittleEndian.AppendUint32(p, b.ConnectionID)

	// string[8]	auth-plugin-data-part-1	first 8 bytes of the plugin provided data (scramble)
	// int<1>	filler	0x00 byte, terminating the first part of a scramble
	// auth-plugin-data 一般来说就是 21 个字符
	// 其中 8 个放在 auth-plugin-data-part1
	// 12 个放在 auth-plugin-data-part2
	// 0 作为结尾
	authPluginData := b.authPluginDataFunc()[:20]
	p = append(p, encoding.NullTerminatedString(authPluginData[:8])...)

	// int<2>	capability_flags_1	The lower 2 bytes of the Capabilities Flags
	// capability part1
	p = append(p, encoding.FixedLengthInteger(uint64(b.CapabilityFlags1), 2)...)

	// int<1>	character_set	default server a_protocol_character_set, only the lower 8-bits
	// 字符集，我们本身并不处理字符，都是透传，所以直接 255
	p = append(p, b.CharacterSet)

	// int<2>	status_flags	SERVER_STATUS_flags_enum
	// 服务器状态，这个目前来看也不知道应该设置什么，从 mysql 服务端里面拿到的最多就是这个
	p = append(p, encoding.FixedLengthInteger(uint64(b.StatusFlags), 2)...)

	// 	int<2>	capability_flags_2	The upper 2 bytes of the Capabilities Flags
	// capability part2
	p = append(p, encoding.FixedLengthInteger(uint64(b.CapabilityFlags2), 2)...)

	if b.capabilities.Has(flags.ClientPluginAuth) {
		// int<1>	auth_plugin_data_len	length of the combined auth_plugin_data (scramble), if auth_plugin_data_len is > 0
		// auth_plugin_data_len 固定是 21，后续可能有变化
		p = append(p, b.AuthPluginDataLength)
	} else {
		// int<1>	00	constant 0x00
		p = append(p, 0x00)
	}

	// string[10]	reserved	reserved. All 0s.
	p = append(p, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

	// $length	auth-plugin-data-part-2
	// Rest of the plugin provided data (scramble), $len=MAX(13, length of auth-plugin-data - 8)
	// 0x00 作为结束符
	p = append(p, encoding.LengthEncodeString(string(encoding.NullTerminatedString(authPluginData[8:])))...)

	if b.capabilities.Has(flags.ClientPluginAuth) {
		// 	NULL	auth_plugin_name	name of the auth_method that the auth_plugin_data belongs to
		// auth plugin name，但是我们作为一个网关，暂时还没啥支持的，
		// 后续要支持不同的 auth name
		// data = append(data)
		p = append(p, encoding.NullTerminatedString(b.AuthPluginName)...)
	}

	return p
}

func AuthPluginDataGenerator() string {
	authPluginData, _ := randx.RandCode(20, randx.TypeMixed)
	return authPluginData
}
