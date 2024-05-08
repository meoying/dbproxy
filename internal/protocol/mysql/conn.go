package mysql

import (
	"fmt"
	"gitee.com/meoying/dbproxy/internal/errs"
	"net"
	"time"
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

	sequence uint8

	id uint32
}

func (mc *Conn) readPacket() ([]byte, error) {
	var prevData []byte
	for {
		// 读取头部的四个字节，其中三个字节是长度，一个字节
		data := make([]byte, 4)
		_, err := mc.conn.Read(data)
		if err != nil {
			_ = mc.Close()
			return nil, fmt.Errorf("%w，读取报文头部失败 %w", errs.ErrInvalidConn, err)
		}

		// packet length [24 bit]
		pktLen := int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16)

		// check packet sync [8 bit]
		// 当我们收到数据的时候，有两种可能
		// 1. 这是一个新命令，那么 sequence
		if data[3] == 0 {
			mc.sequence = 0
		} else if data[3] != mc.sequence {
			// 2. 这是一个老命令，所以我们会预期它的 sequence 应该是我们上次发送完之后 + 1的
			_ = mc.Close()
			return nil, errs.ErrPktSync
		}
		mc.sequence++

		// packets with length 0 terminate a previous packet which is a
		// multiple of (2^24)-1 bytes long
		if pktLen == 0 {
			// there was no previous packet
			if prevData == nil {
				_ = mc.Close()
				return nil, fmt.Errorf("%w，当前报文长度为 0，但未读到前面报文", errs.ErrInvalidConn)
			}

			return prevData, nil
		}

		// read packet body [pktLen bytes]
		body := make([]byte, pktLen)
		_, err = mc.conn.Read(body)
		if err != nil {
			_ = mc.Close()
			return nil, fmt.Errorf("%w，读取报文体失败 %w", errs.ErrInvalidConn, err)
		}

		// return data if this was the last packet
		if pktLen < maxPacketSize {
			// zero allocations for non-split packets
			if prevData == nil {
				return data, nil
			}

			return append(prevData, data...), nil
		}
		prevData = append(prevData, data...)
	}
}

func (mc *Conn) writeOkPacket() error {
	// 0 OK响应
	// 0 影响行数
	// 0 last_insert_id
	// 服务器状态后续我们都要修改
	// 服务器的状态 2 0，warning number 0 0
	return mc.writePacket([]byte{0, 0, 0, 2, 0, 0, 0})
}

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
		if pktLen >= maxPacketSize {
			data[0] = 0xff
			data[1] = 0xff
			data[2] = 0xff
			size = maxPacketSize
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
			if size != maxPacketSize {
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

// cleanup 按照 mysql driver 的说法是为了规避 auth 而引入的
func (mc *Conn) cleanup() {
	_ = mc.conn.Close()
}

func (mc *Conn) Close() error {
	return mc.conn.Close()
}
