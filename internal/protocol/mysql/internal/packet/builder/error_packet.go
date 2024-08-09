package builder

import (
	"encoding/binary"
	"fmt"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
)

// 这里直接照着 MySQL 文档的命令，所以不符合 Go 的规范

// ER_XAER_INVAL 不支持的参数，或者命令
var (
	ER_XAER_INVAL = Error{
		code:     1398,
		sqlState: []byte("XAE05"),
		msg:      "XAER_INVAL: Invalid arguments (or unsupported command)",
	}
)

// Error 表示服务端发生的一个错误
// 这些错误一般都是mysql协议中预定义的错误
// mariadb官方文档中有更好的解释 https://mariadb.com/kb/en/mariadb-error-code-reference/
type Error struct {
	// 错误码
	code uint16
	//  通常固定为五个字符,规则相见上方文档连接
	sqlState []byte
	// 错误信息
	msg string
}

func NewInternalError(cause error) Error {
	return Error{
		// TODO: 这里有问题, 应该针对不同的错误,使用不同的SQLState及描述
		code:     1398,
		sqlState: []byte("HY000"),
		// 占位符，你需要格式化这个数据
		msg: fmt.Sprintf("Internal error: %s", cause),
	}
}

func (e Error) Code() uint16 {
	return e.code
}

func (e Error) SQLState() []byte {
	return e.sqlState
}

func (e Error) Msg() string {
	return e.msg
}

// ErrorPacketBuilder 错误包构建器
type ErrorPacketBuilder struct {

	// capabilities 客户端与服务端建立连接时设置的flags
	Capabilities flags.CapabilityFlags

	// Error 发生的错误
	Error Error
}

func NewErrorPacketBuilder(cap flags.CapabilityFlags, err Error) *ErrorPacketBuilder {
	return &ErrorPacketBuilder{
		Capabilities: cap,
		Error:        err,
	}
}

// Build 构造 ERR_Packet
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html
func (b *ErrorPacketBuilder) Build() []byte {
	// 头部四个字节保留
	p := make([]byte, 4, 13+len(b.Error.Msg()))

	// int<1> header 固定 0xFF 代表错误
	p = append(p, 0xFF)

	// int<2>	error_code	错误码
	p = binary.LittleEndian.AppendUint16(p, b.Error.Code())

	if b.Capabilities.Has(flags.ClientProtocol41) {
		// string[1] sql_state_marker	固定的 # 作为分隔符
		p = append(p, '#')

		// string[5]  sql_state	SQL state
		p = append(p, b.Error.SQLState()...)
	}

	// string<EOF>	error_message 人可读的错误信息
	p = append(p, b.Error.Msg()...)

	return p
}
