package builder

import (
	"fmt"

	"github.com/meoying/dbproxy/internal/errs"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
)

type SetHeader struct {
	sequence uint8
	payload  []byte
}

func NewSetHeader(sequence uint8, payload []byte) *SetHeader {
	return &SetHeader{sequence: sequence, payload: payload}
}

func (b *SetHeader) Build() ([]byte, error) {
	packetLength := len(b.payload) - 4
	if packetLength > packet.MaxPacketSize {
		return nil, fmt.Errorf("%w，最大长度 %d，报文长度 %d",
			errs.ErrPktTooLarge, packet.MaxPacketSize, packetLength)
	}
	b.payload[0] = byte(packetLength)
	b.payload[1] = byte(packetLength >> 8)
	b.payload[2] = byte(packetLength >> 16)
	b.payload[3] = b.sequence
	return b.payload, nil
}
