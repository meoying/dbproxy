package mysql

const (
	// maxPacketSize 单一报文最大长度
	maxPacketSize      = 1<<24 - 1
	minProtocolVersion = 10
)

// https://dev.mysql.com/doc/internals/en/capability-flags.html#packet-Protocol::CapabilityFlags
// 虽然叫做 clientFlag，事实上服务端也是用这些标记位的
type clientFlag uint32

const (
	clientLongPassword clientFlag = 1 << iota
	clientFoundRows
	clientLongFlag
	clientConnectWithDB
	clientNoSchema
	clientCompress
	clientODBC
	clientLocalFiles
	clientIgnoreSpace
	clientProtocol41
	clientInteractive
	clientSSL
	clientIgnoreSIGPIPE
	clientTransactions
	clientReserved
	clientSecureConn
	clientMultiStatements
	clientMultiResults
	clientPSMultiResults
	clientPluginAuth
	clientConnectAttrs
	clientPluginAuthLenEncClientData
	clientCanHandleExpiredPasswords
	clientSessionTrack
	clientDeprecateEOF
)
