package forward

import (
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	pcontext "github.com/meoying/dbproxy/internal/protocol/mysql/plugin/context"
)

// Handler 什么也不做，就是转发请求
// 一般用于测试环境
type Handler struct {
	ds datasource.DataSource
}

func (f *Handler) Handle(ctx *pcontext.Context) (*plugin.Result, error) {
	dml := ctx.ParsedQuery.FirstDML()
	_, ok := dml.GetChildren()[0].(*parser.SelectStatementContext)
	if ok {
		rows, err := f.ds.Query(ctx, datasource.Query{
			SQL:  ctx.Query,
			Args: ctx.Args,
		})
		return &plugin.Result{
			Rows: rows,
		}, err
	}
	res, err := f.ds.Exec(ctx, datasource.Query{
		SQL:  ctx.Query,
		Args: ctx.Args,
	})
	return &plugin.Result{
		Result: res,
	}, err
}

func NewHandler(ds datasource.DataSource) *Handler {
	return &Handler{
		ds: ds,
	}
}
