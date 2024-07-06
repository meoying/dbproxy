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
	ds   datasource.DataSource
	tx   datasource.Tx
	stmt map[int]datasource.Stmt
}

func (f *Handler) Handle(ctx *pcontext.Context) (*plugin.Result, error) {
	var err error
	result := &plugin.Result{}
	sqlStmt := ctx.ParsedQuery.SqlStatement()
	switch typ := sqlStmt.(type) {
	case *parser.TransactionStatementContext:
		err = f.handleTransaction(ctx, typ)
		if err != nil {
			return result, err
		}
		result.ChangeTransaction = true
	case *parser.DmlStatementContext:
		return f.handleDml(ctx, typ)
	case *parser.PreparedStatementContext:
		return f.handlePrepared(ctx, typ)
	}
	return result, nil
}

// handleDml 处理DML语句
func (f *Handler) handleDml(ctx *pcontext.Context, typ *parser.DmlStatementContext) (*plugin.Result, error) {
	switch typ.GetChildren()[0].(type) {
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
func (f *Handler) handleTransaction(ctx *pcontext.Context, typ *parser.TransactionStatementContext) error {
	switch typ.GetChildren()[0].(type) {
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

// handlePrepared 处理预处理相关语句
func (f *Handler) handlePrepared(ctx *pcontext.Context, typ *parser.PreparedStatementContext) (*plugin.Result, error) {
	var err error
	switch typ.GetChildren()[0].(type) {
	case *parser.PrepareStatementContext:
		f.stmt[ctx.StmtId], err = f.ds.Prepare(ctx, ctx.Query)
		return &plugin.Result{
			StmtId: ctx.StmtId,
		}, err
	case *parser.ExecuteStatementContext:
		stmt, ok := f.stmt[ctx.StmtId]
		if !ok {

		}
		rows, err := stmt.Query(ctx, datasource.Query{
			Args: ctx.Args,
		})

		return &plugin.Result{
			Rows: rows,
		}, err
	case *parser.DeallocatePrepareContext:
		stmt, ok := f.stmt[ctx.StmtId]
		if !ok {

		}
		if err = stmt.Close(); err != nil {

		}
		delete(f.stmt, ctx.StmtId)
	}
	return &plugin.Result{}, err
}

func NewHandler(ds datasource.DataSource) *Handler {
	return &Handler{
		ds:   ds,
		stmt: map[int]datasource.Stmt{},
	}
}
