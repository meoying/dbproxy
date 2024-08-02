package packet

import "encoding/binary"

// HandshakeResp 是来自客户端的握手响应
// 包含了头部字段, 去掉头部4个字节, 从第5个字节(编号为4)开始为响应载荷
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html#sect_protocol_connection_phase_packets_protocol_handshake_response41
type HandshakeResp []byte

func (h HandshakeResp) ClientFlags() uint32 {
	return binary.LittleEndian.Uint32(h[4:8])
}

func (h HandshakeResp) CharacterSet() uint32 {
	// 跳过4个字节的 max_packet_size
	return uint32(h[12])
}
