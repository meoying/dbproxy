package flags

// CapabilityFlag
// 这里我们按需定义，只把用到了的添加到这里
// https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__capabilities__flags.html
type CapabilityFlag uint64

const (
	// ClientProtocol41  New 4.1 protocol
	ClientProtocol41 CapabilityFlag = 512

	// ClientTransactions
	// Client knows about transactions
	ClientTransactions = 8192

	// ClientPluginAuth
	// Client supports plugin authentication.
	ClientPluginAuth = 1 << 19

	// ClientSessionTrack
	// Capable of handling server state change information
	ClientSessionTrack = 1 << 23

	// ClientDeprecateEOF
	// Client no longer needs EOF_Packet and will use OK_Packet instead.
	ClientDeprecateEOF = 1 << 24

	// ClientOptionalResultsetMetadata
	// The client can handle optional metadata information in the resultset.
	ClientOptionalResultsetMetadata = 1 << 25

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
