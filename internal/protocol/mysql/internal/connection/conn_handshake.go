package connection

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/builder"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/parser"
)

// startHandshake
// 在 mysql 协议中，在建立了 TCP 连接之后
// mysql server 端发起 startHandshake
// 而后客户端要响应 startHandshake
func (mc *Conn) startHandshake() error {
	b := builder.NewHandshakeV10Packet(flags.CapabilityFlags(flags.ClientPluginAuth), flags.ServerStatusAutoCommit, builder.AuthPluginDataGenerator)
	b.ProtocolVersion = packet.MinProtocolVersion
	b.ServerVersion = "8.4.0"
	b.ConnectionID = mc.id
	b.AuthPluginName = "mysql_native_password"
	return mc.WritePacket(b.Build())
}

func (mc *Conn) auth() error {
	payload, err := mc.readPacket()
	if err != nil {
		return err
	}
	// TODO: 这里不该默认用41解析,需要根据客户端传递的flags来判断一下
	p := parser.HandshakeResponse41{}
	err = p.Parse(payload)
	if err != nil {
		return err
	}
	mc.clientFlags = p.ClientFlags()
	mc.characterSet = p.CharacterSet()
	// 写回 OK 响应
	b := builder.NewOKPacket(mc.ClientCapabilityFlags(), flags.ServerStatusAutoCommit)
	return mc.WritePacket(b.Build())
}
