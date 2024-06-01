package sharding

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor"
	"github.com/meoying/dbproxy/internal/sharding"
)

type ShardingHandler interface {
	visitor.Visitor
	sharding.Executor
	sharding.QueryBuilder
}
