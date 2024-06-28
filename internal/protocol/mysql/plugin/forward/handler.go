package forward

import (
	"database/sql"

	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

// Handler 什么也不做，就是转发请求
// 一般用于测试环境
type Handler struct {
	ds datasource.DataSource
	tx datasource.Tx
}

func (f *Handler) Handle(ctx *pcontext.Context) (*plugin.Result, error) {
	var err error
	result := &plugin.Result{}
	sqlStmt := ctx.ParsedQuery.SqlStatement()
	switch typ := sqlStmt.(type) {
	case *parser.TransactionStatementContext:
		err = f.handleTransaction(ctx, typ)
		result.ChangeTransaction = true
		return result, err
	case *parser.DmlStatementContext:
		return f.handleDml(ctx, typ)
	}
	return result, nil
}

// handleDml 处理DML语句
func (f *Handler) handleDml(ctx *pcontext.Context, stmt *parser.DmlStatementContext) (*plugin.Result, error) {
	switch stmt.GetChildren()[0].(type) {
	case *parser.SimpleSelectContext:
		return f.handleSelect(ctx)
	case *parser.InsertStatementContext:
		return f.handleCUD(ctx)
	case *parser.UpdateStatementContext:
		return f.handleCUD(ctx)
	case *parser.DeleteStatementContext:
		return f.handleCUD(ctx)
	}
	return &plugin.Result{}, nil
}

// handleSelect 处理Select语句
func (f *Handler) handleSelect(ctx *pcontext.Context) (*plugin.Result, error) {
	var rows *sql.Rows
	var err error
	if ctx.InTransition {
		rows, err = f.tx.Query(ctx, datasource.Query{
			SQL:  ctx.Query,
			Args: ctx.Args,
		})
	} else {
		rows, err = f.ds.Query(ctx, datasource.Query{
			SQL:  ctx.Query,
			Args: ctx.Args,
		})
	}

	return &plugin.Result{
		Rows: rows,
	}, err
}

// handleCUD 操作数据
func (f *Handler) handleCUD(ctx *pcontext.Context) (*plugin.Result, error) {
	var err error
	var res sql.Result
	if ctx.InTransition {
		res, err = f.tx.Exec(ctx, datasource.Query{
			SQL:  ctx.Query,
			Args: ctx.Args,
		})
	} else {
		res, err = f.ds.Exec(ctx, datasource.Query{
			SQL:  ctx.Query,
			Args: ctx.Args,
		})
	}

	return &plugin.Result{
		Result: res,
	}, err
}

// handleTransaction 处理事务相关语句
func (f *Handler) handleTransaction(ctx *pcontext.Context, stmt *parser.TransactionStatementContext) error {
	switch stmt.GetChildren()[0].(type) {
	case *parser.StartTransactionContext:
		var err error
		f.tx, err = f.ds.BeginTx(ctx, nil)
		return err
	case *parser.CommitWorkContext:
		return f.tx.Commit()
	case *parser.RollbackWorkContext:
		return f.tx.Rollback()
	}
	return nil
}

func NewHandler(ds datasource.DataSource) *Handler {
	return &Handler{
		ds: ds,
	}
}
