package packet

// 这里直接照着 MySQL 文档的命令，所以不符合 Go 的规范

// ER_XAER_INVAL
// 不支持的参数，或者命令
var ER_XAER_INVAL = ErrorResp{
	code:  1398,
	state: []byte("XAE05"),
	msg:   "XAER_INVAL: Invalid arguments (or unsupported command)",
}

type ErrorResp struct {
	code uint16
	// 固定为五个字符
	state []byte
	msg   string
}
