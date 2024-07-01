package connection

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
)

// OnCmd 返回是否处理成功
type OnCmd func(ctx context.Context, conn *Conn, payload []byte) error

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
	Id           uint32

	// onCmd 处理客户端过来的命令
	onCmd        OnCmd
	cmdTimeout   time.Duration
	InTransition bool

	clientFlags  flags.CapabilityFlags
	characterSet uint32
}

func NewConn(id uint32, rc net.Conn, onCmd OnCmd) *Conn {
	return &Conn{
		conn:             rc,
		maxAllowedPacket: maxPacketSize,
		// 后续要考虑做成可配置的
		writeTimeout: time.Second * 3,
		onCmd:        onCmd,
		Id:           id,
		cmdTimeout:   time.Second * 3,
	}
}

// Loop 完成握手、鉴权，并且开始监听客户端的数据
// 返回错误之后，则意味着这个 Conn 已经不可用
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
		pkt, err1 := mc.readPacket()
		if err1 != nil {
			log.Println(err1, "xxxxxxxxxxxxxxx")
			return fmt.Errorf("读取客户端请求失败 %w", err)
		}
		//ctx, _ := context.WithTimeout(context.Background(), mc.cmdTimeout)
		ctx := context.Background()
		err1 = mc.onCmd(ctx, mc, pkt)
		//cancel() // TODO：暂时注释，因为这个会影响事务自动回滚，还不清楚原因
		if err1 != nil {
			return err1
		}
	}
}

func (mc *Conn) Close() error {
	return mc.conn.Close()
}

func (mc *Conn) ClientCapabilityFlags() flags.CapabilityFlags {
	return mc.clientFlags
}

func (mc *Conn) CharacterSet() uint32 {
	return mc.characterSet
}
