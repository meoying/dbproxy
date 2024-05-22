package packet

import "encoding/binary"

// HandshakeResp 是来自客户端的握手响应
// 包含了头部字段
type HandshakeResp []byte

func (h HandshakeResp) ClientFlags() uint32 {
	return binary.LittleEndian.Uint32(h[5:9])
}

func (h HandshakeResp) CharacterSet() uint32 {
	return uint32(h[9])
}
