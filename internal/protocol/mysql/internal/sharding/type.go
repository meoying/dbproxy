package sharding

import (
	"context"
	"database/sql"

	"github.com/ecodeclub/ekit/sqlx"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/sharding"
)

type ShardingHandler interface {
	// Build 构建分库分表的sql
	// 这个方法是不是用不上了，我看到只在测试里面用了
	Build(ctx context.Context) ([]sharding.Query, error)
	QueryOrExec(ctx context.Context) (*Result, error)
}

type NewHandlerFunc func(a sharding.Algorithm, db datasource.DataSource, ctx *pcontext.Context) (ShardingHandler, error)

type Result struct {
	// 这两个字段中只能有一个
	// Rows 的 error 会被传递过去客户端
	Rows sqlx.Rows
	// Result 的 error 会被传递过去客户端
	Result sql.Result

	TxInTransaction bool
}
