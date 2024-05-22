package errs

import (
	"errors"
	"fmt"
)

var ErrMissingShardingKey = errors.New("sharding key 未设置")

func NewUnsupportedOperatorError(op string) error {
	return fmt.Errorf("不支持的操作符 %v", op)
}
