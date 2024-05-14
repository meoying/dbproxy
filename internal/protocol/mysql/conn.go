package mysql

import (
	"fmt"
	"net"
	"time"

	"gitee.com/meoying/dbproxy/internal/protocol/mysql/internal/cmd"
	"gitee.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"gitee.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
)

// Conn 代表了 MySQL 的一个连接
// 要参考 mysql driver 的设计与实现
// 但是我个人觉得它的写法并不是特别优雅
type Conn struct {
	conn net.Conn
	// 默认是 maxPacketSize
	maxAllowedPacket int
	// 写入超时时间
	writeTimeout time.Duration
	sequence     uint8
	id           uint32
	executors    map[byte]cmd.Executor

	clientFlags flags.CapabilityFlags
}

func newConn(id uint32, rc net.Conn) *Conn {
	return &Conn{
		conn:             rc,
		maxAllowedPacket: maxPacketSize,
		// 后续要考虑做成可配置的
		writeTimeout: time.Second * 3,
		id:           id,
		executors: map[byte]cmd.Executor{
			cmd.CmdPing.Byte():  &cmd.PingExecutor{},
			cmd.CmdQuery.Byte(): &cmd.QueryExecutor{},
		},
	}
}

func (mc *Conn) Loop() error {
	// 先建立连接
	err := mc.startHandshake()
	if err != nil {
		return fmt.Errorf("发送握手请求失败 %w", err)
	}
	// 鉴权
	err = mc.auth()
	if err != nil {
		return fmt.Errorf("开始鉴权失败 %w", err)
	}
	for {
		// 开始不断接收客户端的请求
		pkt, err := mc.readPacket()
		if err != nil {
			return fmt.Errorf("读取客户端请求失败 %w", err)
		}
		// 第一个字节是命令
		exec, ok := mc.executors[pkt[0]]
		var resp []byte
		if ok {
			cmdCtx := &cmd.Context{
				CapabilityFlags: mc.clientFlags,
			}
			// TODO 这里返回的是一个[][]byte多个包，目前一时没有想到怎么去改writePacket这个方法去组装到一起，所以先写了一个备注
			_, err := exec.Exec(cmdCtx, pkt)
			if err != nil {
				return err
			}
		} else {
			resp = packet.BuildErrRespPacket(packet.ER_XAER_INVAL)
		}
		err = mc.writePacket(resp)
		if err != nil {
			return err
		}
	}
}

// cleanup 按照 mysql driver 的说法是为了规避 auth 而引入的
func (mc *Conn) cleanup() {
	_ = mc.conn.Close()
}

func (mc *Conn) Close() error {
	return mc.conn.Close()
}
