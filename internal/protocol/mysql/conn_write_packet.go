package mysql

import (
	"fmt"
	"time"

	"gitee.com/meoying/dbproxy/internal/errs"
)

// Write packet buffer 'data'
func (mc *Conn) writePacket(data []byte) error {
	pktLen := len(data) - 4

	if pktLen > mc.maxAllowedPacket {
		return fmt.Errorf("%w，最大长度 %d，报文长度 %d",
			errs.ErrPktTooLarge,
			mc.maxAllowedPacket, pktLen)
	}

	for {
		var size int
		if pktLen >= flags.maxPacketSize {
			data[0] = 0xff
			data[1] = 0xff
			data[2] = 0xff
			size = flags.maxPacketSize
		} else {
			data[0] = byte(pktLen)
			data[1] = byte(pktLen >> 8)
			data[2] = byte(pktLen >> 16)
			size = pktLen
		}
		data[3] = mc.sequence

		// Write packet
		if mc.writeTimeout > 0 {
			if err := mc.conn.SetWriteDeadline(time.Now().Add(mc.writeTimeout)); err != nil {
				return err
			}
		}

		n, err := mc.conn.Write(data[:4+size])
		if err == nil && n == 4+size {
			mc.sequence++
			if size != flags.maxPacketSize {
				return nil
			}
			pktLen -= size
			data = data[size:]
			continue
		}

		// 到这里就是写入失败。有两种情况，一种是 err 不为 nil
		// 一种是写入数据的长度不够
		// 在这种情况下，需要做一些清理工作

		mc.cleanup()
		return errs.ErrInvalidConn
	}
}

func (mc *Conn) writeCommandPacket(command byte) error {
	// 重置这个 sequence。在 MySQL 里面，sequence 是一个命令内部独立计数的
	// 但是一个命令可能会读写多次数据，所以就比较恶心，必须用字段来维护
	mc.sequence = 0
	// 四个字节 + 命令（一个字节）
	data := make([]byte, 5)
	// Add command byte
	data[4] = command

	// Send CMD packet
	return mc.writePacket(data)
}
