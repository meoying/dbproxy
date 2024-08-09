package connection

import (
	"fmt"
	"time"

	"github.com/meoying/dbproxy/internal/errs"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/builder"
)

// WritePacket 写入一个 packet
// https://mariadb.com/kb/en/0-packet/
// 注意：
// 1. WritePacket 并不会执行拆包。也就是说如果你的 data 需要多个报文来发送，那么你需要自己手动拆分
// 2. 你需要在 data 里面预留出来四个字节的头部字段
func (mc *Conn) WritePacket(data []byte) error {

	data, err := builder.NewSetHeader(mc.sequence, data).Build()
	if err != nil {
		return err
	}

	// 设置回写的超时时间
	if mc.writeTimeout > 0 {
		if err := mc.conn.SetWriteDeadline(time.Now().Add(mc.writeTimeout)); err != nil {
			return err
		}
	}

	// log.Printf(">>>> WritePaket = %#v\n", data)
	n, err := mc.conn.Write(data)

	// 到这里就是写入失败。有两种情况，一种是 err 不为 nil
	// 一种是写入数据的长度不够
	if err != nil {
		return fmt.Errorf("%w: 写入数据失败，原因 %w", errs.ErrInvalidConn, err)
	}

	if n != len(data) {
		return fmt.Errorf("%w: 写入数据失败, 未写入足够数据，预期写入：%d，实际写入：%d", errs.ErrInvalidConn, len(data), n)
	}
	mc.sequence++
	return nil
}
