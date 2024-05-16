package connection

import (
	"fmt"

	"github.com/meoying/dbproxy/internal/errs"
)

// readPacket 读取一个完整报文，已经去除了头部字段，只剩下 payload 字段
func (mc *Conn) readPacket() ([]byte, error) {
	var prevData []byte
	for {
		// 读取头部的四个字节，其中三个字节是长度，一个字节是 sequence
		data := make([]byte, 4)
		_, err := mc.conn.Read(data)
		if err != nil {
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
				return nil, fmt.Errorf("%w，当前报文长度为 0，但未读到前面报文", errs.ErrInvalidConn)
			}
			return prevData, nil
		}

		// read packet body [pktLen bytes]
		body := make([]byte, pktLen)
		_, err = mc.conn.Read(body)
		if err != nil {
			return nil, fmt.Errorf("%w，读取报文体失败 %w", errs.ErrInvalidConn, err)
		}

		// return data if this was the last packet
		if pktLen < maxPacketSize {
			// zero allocations for non-split packets
			if prevData == nil {
				return body, nil
			}

			return append(prevData, body...), nil
		}
		prevData = append(prevData, body...)
	}
}
