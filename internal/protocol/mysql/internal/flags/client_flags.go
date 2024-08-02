package flags

// CapabilityFlag
// 这里我们按需定义，只把用到了的添加到这里
// https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__capabilities__flags.html
type CapabilityFlag uint64

const (
	// CLIENT_PROTOCOL_41  New 4.1 protocol
	CLIENT_PROTOCOL_41 CapabilityFlag = 512

	// CLIENT_DEPRECATE_EOF
	// Client no longer needs EOF_Packet and will use OK_Packet instead.
	CLIENT_DEPRECATE_EOF = 1 << 24

	// CLIENT_OPTIONAL_RESULTSET_METADATA
	// The client can handle optional metadata information in the resultset.
	CLIENT_OPTIONAL_RESULTSET_METADATA = 1 << 25

	// ClientQueryAttributes
	// Support optional extension for query parameters into the COM_QUERY and COM_STMT_EXECUTE packets.
	ClientQueryAttributes = 1 << 27
)

// CapabilityFlags 是客户端告诉服务端，它支持什么样的功能特性
// 通过 connection.Conn 的 ClientCapabilityFlags 方法获取该信息
type CapabilityFlags uint64

func (flags CapabilityFlags) Has(flag CapabilityFlag) bool {
	return uint64(flags)&uint64(flag) > 0
}
