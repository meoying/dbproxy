package sharding

import "github.com/pkg/errors"

var ErrInsertShardingKeyNotFound         = errors.New("eorm: insert语句中未包含sharding key")