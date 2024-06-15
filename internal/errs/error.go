package errs

import (
	"errors"
)

var ErrInvalidConn = errors.New("异常连接")
var ErrPktSync = errors.New("报文乱序")
var ErrPktTooLarge = errors.New("报文过大")

