package sharding

import (
	"context"

	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	"github.com/meoying/dbproxy/internal/sharding"
)

type ShardingHandler interface {
	// 构建分库分表的sql
	Build(ctx context.Context) ([]sharding.Query, error)
	QueryOrExec(ctx context.Context) (*plugin.Result, error)
}

type NewHandlerFunc func(a sharding.Algorithm, db datasource.DataSource, ctx *pcontext.Context) (ShardingHandler, error)
