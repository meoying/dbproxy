package plugin

import (
	"context"
	"database/sql"
	"github.com/ecodeclub/ekit/sqlx"
)

// Plugin 顶级接口，用户接入的
type Plugin interface {
	// Handle 你需要同时处理 query 中直接拼接查询参数，以及使用占位符这两种情况
	// query 可能是增删改查，或者其他语句
	Handle(ctx context.Context, query string, args ...any) (Result, error)
}

type Result struct {
	// 只能有两个字段的中的一个
	// Rows 查询类的结果
	Rows sqlx.Rows
	// Result 执行类的结果
	Result sql.Result
}
