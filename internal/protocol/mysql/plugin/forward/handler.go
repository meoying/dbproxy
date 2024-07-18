package forward

import (
	"database/sql"
	"fmt"

	"github.com/ecodeclub/ekit/syncx"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/protocol/mysql/driver/sharding"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

// Handler 什么也不做，就是转发请求
// 一般用于测试环境
// 这个实现有一个巨大的问题，即 Handler 不是线程安全的
// TODO 后续要考虑多个事务（不同的 Connection) 同时执行的问题
type Handler struct {
	ds        datasource.DataSource
	connID2Tx syncx.Map[uint32, datasource.Tx]
}

func (f *Handler) Handle(ctx *pcontext.Context) (*plugin.Result, error) {
	sqlStmt := ctx.ParsedQuery.SqlStatement()
	switch typ := sqlStmt.(type) {
	case *parser.TransactionStatementContext:
		return f.handleTransactionStmt(ctx, typ)
	case *parser.DmlStatementContext:
		return f.handleDmlStmt(ctx, typ)
	default:
		return &plugin.Result{}, fmt.Errorf("未知SQL语句: %T", typ)
	}
}

// handleDmlStmt 处理DML语句
func (f *Handler) handleDmlStmt(ctx *pcontext.Context, stmt *parser.DmlStatementContext) (*plugin.Result, error) {
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
	if tx := f.getTx(ctx.ConnID); tx != nil {
		rows, err = tx.Query(ctx, datasource.Query{
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

func (f *Handler) getTx(connID uint32) datasource.Tx {
	if tx, ok := f.connID2Tx.Load(connID); ok {
		return tx
	}
	return nil
}

// handleCUD 操作数据
func (f *Handler) handleCUD(ctx *pcontext.Context) (*plugin.Result, error) {
	var err error
	var res sql.Result
	if tx := f.getTx(ctx.ConnID); tx != nil {
		// 事务中
		res, err = tx.Exec(ctx, datasource.Query{
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

// handleTransactionStmt 处理事务相关语句
func (f *Handler) handleTransactionStmt(ctx *pcontext.Context, stmt *parser.TransactionStatementContext) (*plugin.Result, error) {
	var result plugin.Result
	var err error
	var tx datasource.Tx
	switch stmt.GetChildren()[0].(type) {
	case *parser.StartTransactionContext:
		tx, err = f.ds.BeginTx(sharding.NewSingleTxContext(ctx), nil)
		if err == nil {
			f.connID2Tx.Store(ctx.ConnID, tx)
			result.TxInTransaction = true
		}
	case *parser.CommitWorkContext:
		tx = f.getTx(ctx.ConnID)
		if tx != nil {
			err = tx.Commit()
		}
		f.connID2Tx.Delete(ctx.ConnID)
	case *parser.RollbackWorkContext:
		tx = f.getTx(ctx.ConnID)
		if tx != nil {
			err = tx.Rollback()
		}
		f.connID2Tx.Delete(ctx.ConnID)
	default:
		err = fmt.Errorf("未知事务语句")
	}
	return &result, err
}

func NewHandler(ds datasource.DataSource) *Handler {
	return &Handler{
		ds: ds,
	}
}
