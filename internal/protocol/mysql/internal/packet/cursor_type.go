package packet

// CursorType
// https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#a3e5e9e744ff6f7b989a604fd669977da
type CursorType byte

const (
	CURSOR_TYPE_NO_CURSOR  CursorType = 0
	CURSOR_TYPE_READ_ONLY             = 1
	CURSOR_TYPE_FOR_UPDATE            = 2
	CURSOR_TYPE_SCROLLABLE            = 4
	// PARAMETER_COUNT_AVAILABLE  当客户端发送参数数量即使为0也开启该选项
	PARAMETER_COUNT_AVAILABLE = 8
)
