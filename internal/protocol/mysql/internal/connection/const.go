package connection

const (
	// maxPacketSize 单一报文最大长度
	maxPacketSize      = 1<<24 - 1
	minProtocolVersion = 10
)
