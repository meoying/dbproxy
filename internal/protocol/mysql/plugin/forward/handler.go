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

func (h *Handler) Handle(ctx *pcontext.Context) (*plugin.Result, error) {
	sqlStmt := ctx.ParsedQuery.SqlStatement()
	switch typ := sqlStmt.(type) {
	case *parser.TransactionStatementContext:
		return h.handleTransactionStmt(ctx, typ)
	case *parser.DmlStatementContext:
		return h.handleDmlStmt(ctx, typ)
	default:
		return &plugin.Result{}, fmt.Errorf("未知SQL语句: %T", typ)
	}
}

// handleDmlStmt 处理DML语句
func (h *Handler) handleDmlStmt(ctx *pcontext.Context, stmt *parser.DmlStatementContext) (*plugin.Result, error) {
	switch stmt.GetChildren()[0].(type) {
	case *parser.SimpleSelectContext:
		return h.handleSelectStmt(ctx)
	case *parser.InsertStatementContext:
		return h.handleCUDStmt(ctx)
	case *parser.UpdateStatementContext:
		return h.handleCUDStmt(ctx)
	case *parser.DeleteStatementContext:
		return h.handleCUDStmt(ctx)
	}
	return &plugin.Result{}, nil
}

// handleSelectStmt 处理Select语句
func (h *Handler) handleSelectStmt(ctx *pcontext.Context) (*plugin.Result, error) {
	var rows *sql.Rows
	var err error
	if tx := h.getTx(ctx.ConnID); tx != nil {
		rows, err = tx.Query(ctx, datasource.Query{
			SQL:  ctx.Query,
			Args: ctx.Args,
		})
	} else {
		rows, err = h.ds.Query(ctx, datasource.Query{
			SQL:  ctx.Query,
			Args: ctx.Args,
		})
	}

	return &plugin.Result{
		Rows: rows,
	}, err
}

func (h *Handler) getTx(connID uint32) datasource.Tx {
	if tx, ok := h.connID2Tx.Load(connID); ok {
		return tx
	}
	return nil
}

// handleCUDStmt 操作数据
func (h *Handler) handleCUDStmt(ctx *pcontext.Context) (*plugin.Result, error) {
	var err error
	var res sql.Result
	if tx := h.getTx(ctx.ConnID); tx != nil {
		// 事务中
		res, err = tx.Exec(ctx, datasource.Query{
			SQL:  ctx.Query,
			Args: ctx.Args,
		})
	} else {
		res, err = h.ds.Exec(ctx, datasource.Query{
			SQL:  ctx.Query,
			Args: ctx.Args,
		})
	}

	return &plugin.Result{
		Result: res,
	}, err
}

// handleTransactionStmt 处理事务相关语句
func (h *Handler) handleTransactionStmt(ctx *pcontext.Context, stmt *parser.TransactionStatementContext) (*plugin.Result, error) {
	var result plugin.Result
	var err error
	var tx datasource.Tx
	switch stmt.GetChildren()[0].(type) {
	case *parser.StartTransactionContext:
		tx, err = h.ds.BeginTx(sharding.NewSingleTxContext(ctx), nil)
		if err == nil {
			h.connID2Tx.Store(ctx.ConnID, tx)
			result.TxInTransaction = true
		}
	case *parser.CommitWorkContext:
		tx = h.getTx(ctx.ConnID)
		if tx != nil {
			err = tx.Commit()
		}
		h.connID2Tx.Delete(ctx.ConnID)
	case *parser.RollbackWorkContext:
		tx = h.getTx(ctx.ConnID)
		if tx != nil {
			err = tx.Rollback()
		}
		h.connID2Tx.Delete(ctx.ConnID)
	default:
		err = fmt.Errorf("未知事务语句")
	}
	return &result, err
}

func newHandler(ds datasource.DataSource) *Handler {
	return &Handler{
		ds: ds,
	}
}
