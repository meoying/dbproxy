package sharding

import (
	"errors"
	"fmt"
)

var ErrUnKnowSql = errors.New("未知的sql")

func NewErrUnKnowSelectCol(col string) error {
	return fmt.Errorf("select列表中未找到列 %s", col)
}

var ErrInsertShardingKeyNotFound = errors.New(" insert语句中未包含sharding key")
