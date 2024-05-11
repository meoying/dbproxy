package flags

// CapabilityFlags 是客户端告诉服务端，它支持什么样的功能特性
type CapabilityFlags uint64

func (flags CapabilityFlags) Has(flag CapabilityFlag) bool {
	return uint64(flags)&uint64(flag) > 0
}

// CapabilityFlag
// 这里我们按需定义，只把用到了的添加到这里
type CapabilityFlag uint64

const (
	ClientQueryAttributes CapabilityFlag = 1 << 27
)
