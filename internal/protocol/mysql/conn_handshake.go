package mysql

// 开始初始化
// 在 mysql 协议中，在建立了 TCP 连接之后
// mysql server 端发起 handshake
// 而后客户端要响应 handshake
// TODO 支持 SSL
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase.html#sect_protocol_connection_phase_initial_handshake
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
func (mc *Conn) handShake() {
	mc.writeCommandPacket(com)
}
