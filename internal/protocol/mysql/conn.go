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
// 1. 抛弃了使用 sequence 字段的做法。那种做法会导致整个代码中 sequence 在多出操作，晦涩难懂。
// 2. 不支持兼容模式，也就是无法兼容老的客户端访问的请求，这样可以规避很多不必要的麻烦
type Conn struct {
	conn net.Conn
	// 默认是 maxPacketSize
	maxAllowedPacket int
	// 写入超时时间
	writeTimeout time.Duration
}

func (mc *Conn) readPacket() ([]byte, error) {
	var prevData []byte
	var sequence uint8 = 0
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
		if data[3] != sequence {
			_ = mc.Close()
			return nil, errs.ErrPktSync
		}
		sequence++

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

// Write packet buffer 'data'
func (mc *Conn) writePacket(data []byte) error {
	pktLen := len(data) - 4

	if pktLen > mc.maxAllowedPacket {
		return fmt.Errorf("%w，最大长度 %d，报文长度 %d",
			errs.ErrPktTooLarge,
			mc.maxAllowedPacket, pktLen)
	}

	var sequence uint8 = 0

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
		data[3] = sequence

		// Write packet
		if mc.writeTimeout > 0 {
			if err := mc.conn.SetWriteDeadline(time.Now().Add(mc.writeTimeout)); err != nil {
				return err
			}
		}

		n, err := mc.conn.Write(data[:4+size])
		if err == nil && n == 4+size {
			sequence++
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
	// Reset Packet Sequence
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
