package parser

import (
	"encoding/binary"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
)

// HandshakeResponse41 是来自客户端的握手响应
// 包含了头部字段, 去掉头部4个字节, 从第5个字节(编号为4)开始为响应载荷
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html#sect_protocol_connection_phase_packets_protocol_handshake_response41
type HandshakeResponse41 struct {
	// int<4>	client_flag	Capabilities Flags, CLIENT_PROTOCOL_41 always set.
	clientFlag flags.CapabilityFlags

	// int<4>	max_packet_size	maximum packet size

	// int<1>	character_set	client charset a_protocol_character_set, only the lower 8-bits
	characterSet uint32
}

func (h *HandshakeResponse41) Parse(payload []byte) error {
	h.clientFlag = flags.CapabilityFlags(flags.ClientProtocol41)
	h.clientFlag |= flags.CapabilityFlags(binary.LittleEndian.Uint32(payload[4:8]))
	h.characterSet = uint32(payload[12])
	return nil
}

func (h *HandshakeResponse41) ClientFlags() flags.CapabilityFlags {
	return h.clientFlag
}

func (h *HandshakeResponse41) CharacterSet() uint32 {
	// 跳过4个字节的 max_packet_size
	return h.characterSet
}
