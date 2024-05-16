package forward

import (
	"context"
	"database/sql"
	"github.com/meoying/dbproxy/internal/plugin"
	"strings"
)

// Handler 什么也不做，就是转发请求
// 一般用于测试环境
type Handler struct {
	DB *sql.DB
}

func (f *Handler) Handle(ctx context.Context, query string, args ...any) (plugin.Result, error) {
	if strings.HasPrefix(query, "SELECT") ||
		strings.HasPrefix(query, "select") {
		rows, err := f.DB.QueryContext(ctx, query, args...)
		return plugin.Result{
			Rows: rows,
		}, err
	} else {
		res, err := f.DB.ExecContext(ctx, query, args...)
		return plugin.Result{
			Result: res,
		}, err
	}
}
