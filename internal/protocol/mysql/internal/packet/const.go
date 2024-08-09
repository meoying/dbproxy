package packet

// 字符编码类型
const (
	CharSetUtf8mb4GeneralCi uint32 = 45
	CharSetBinary           uint32 = 63
)

const (
	// MaxPacketSize 单一报文最大长度
	MaxPacketSize      = 1<<24 - 1
	MinProtocolVersion = 10
)
