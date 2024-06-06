package sharding

import (
	"fmt"
	"github.com/pkg/errors"
)

var ErrInsertShardingKeyNotFound = errors.New(" insert语句中未包含sharding key")

func NewErrUpdateShardingKeyUnsupported(field string) error {
	return fmt.Errorf("ShardingKey `%s` 不支持更新", field)
}

var ErrUnsupportedAssignment = errors.New(" 不支持的 assignment")
