package packet

// SeverStatus MySQL 中代表服务器状态的枚举
// https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#a1d854e841086925be1883e4d7b4e8cad
type SeverStatus uint64

// AsUint16 目前在协议中都是用两个字节来传输的
func (s SeverStatus) AsUint16() uint16 {
	return uint16(s)
}

// 4 这个值并没有定义
const (
	SeverStatusInTrans             SeverStatus = 1
	ServerStatusAutoCommit         SeverStatus = 2
	ServerMoreResultsExists        SeverStatus = 8
	ServerQueryNoGoodIndexUsed     SeverStatus = 16
	ServerQueryNoIndexUsed         SeverStatus = 32
	ServerStatusCursorExists       SeverStatus = 64
	ServerStatusLastRowSent        SeverStatus = 128
	ServerStatusDBDropped          SeverStatus = 256
	ServerStatusNoBackSlashEscapes SeverStatus = 512
	ServerStatusMetadataChanged    SeverStatus = 1024
	ServerQueryWasSlow             SeverStatus = 2048
	ServerPsOutParams              SeverStatus = 4096
	ServerStatusInTransReadOnly    SeverStatus = 8192
	ServerSessionStateChanged      SeverStatus = 16384
)
