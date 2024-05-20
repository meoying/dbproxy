package connection

import (
	"encoding/binary"

	"github.com/ecodeclub/ekit/randx"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
)

// 开始初始化
// 在 mysql 协议中，在建立了 TCP 连接之后
// mysql server 端发起 startHandshake
// 而后客户端要响应 startHandshake
// TODO 支持 SSL
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase.html#sect_protocol_connection_phase_initial_handshake
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
// 可以参考 README 中的一个例子
func (mc *Conn) startHandshake() error {
	// 报文比较复杂
	data := make([]byte, 1)
	// 设置协议版本
	data[0] = minProtocolVersion
	// 这里我们将自己定义为是 8.4.0 的版本
	data = append(data, []byte("8.4.0")...)
	// 版本结束标记位
	data = append(data, 0)

	// thread Id 或者说 connection Id
	data = binary.LittleEndian.AppendUint32(data, mc.Id)

	// auth-plugin-data 一般来说就是 21 个字符
	// 其中 8 个放在 auth-plugin-data-part1
	// 12 个放在 auth-plugin-data-part2
	// 0 作为结尾
	code, _ := randx.RandCode(20, randx.TypeMixed)
	data = append(data, code[:8]...)
	data = append(data, 0)
	// capability part1
	data = append(data, 255, 255)
	// 字符集，我们本身并不处理字符，都是透传，所以直接 255
	data = append(data, 255)
	// 服务器状态，这个目前来看也不知道应该设置什么，从 mysql 服务端里面拿到的最多就是这个
	data = append(data, 2, 0)
	// capability part2
	data = append(data, 255, 223)
	// auth_plugin_data_len 固定是 21，后续可能有变化
	data = append(data, 21)
	data = append(data, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0)
	data = append(data, code[8:]...)
	data = append(data, 0)
	// auth plugin name，但是我们作为一个网关，暂时还没啥支持的，
	// 后续要支持不同的 auth name
	// data = append(data)
	return mc.WritePacket(data)
}

// readHandshakeResp 读取客户端在 startHandshake 中返回来的响应
func (mc *Conn) readHandshakeResp() (packet.HandshakeResp, error) {
	data, err := mc.readPacket()
	return data, err
}

func (mc *Conn) auth() error {
	// 后续真的执行鉴权，就要处理这里读取到的 data
	resp, err := mc.readHandshakeResp()
	if err != nil {
		return err
	}
	mc.clientFlags = flags.CapabilityFlags(resp.ClientFlags())
	mc.characterSet = resp.CharacterSet()
	// 写回 OK 响应
	return mc.WritePacket(packet.BuildOKResp(packet.ServerStatusAutoCommit))
}
