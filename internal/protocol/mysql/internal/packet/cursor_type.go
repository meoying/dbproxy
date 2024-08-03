package packet

// CursorType
// https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#a3e5e9e744ff6f7b989a604fd669977da
type CursorType byte

const (
	// ParameterCountAvailable  当客户端发送参数数量即使为0也开启该选项
	ParameterCountAvailable CursorType = 8
)
