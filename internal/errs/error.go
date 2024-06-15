package errs

import (
	"errors"
	"fmt"
)

var ErrInvalidConn = errors.New("异常连接")
var ErrPktSync = errors.New("报文乱序")
var ErrPktTooLarge = errors.New("报文过大")

func NewErrScanWrongDestinationArguments(expect int, actual int) error {
	return fmt.Errorf("dbproxy: Scan 方法收到过多或者过少的参数，预期 %d，实际 %d", expect, actual)
}
