package forward

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/ecodeclub/ekit/syncx"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/protocol/mysql/driver/sharding"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/vparser"
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

func newHandler(ds datasource.DataSource) *Handler {
	return &Handler{
		ds: ds,
	}
}

func (h *Handler) Handle(ctx *pcontext.Context) (*plugin.Result, error) {
	visitor := vparser.NewCheckVisitor()
	stmtType := visitor.Visit(ctx.ParsedQuery.Root).(string)
	switch stmtType {
	case vparser.SelectStmt:
		return h.handleSelectStmt(ctx)
	case vparser.InsertStmt, vparser.UpdateStmt, vparser.DeleteStmt:
		return h.handleCUDStmt(ctx)
	case vparser.StartTransactionStmt:
		return h.handleStartTransactionStmt(ctx)
	case vparser.CommitStmt:
		return h.handleCommitStmt(ctx)
	case vparser.RollbackStmt:
		return h.handleRollbackStmt(ctx)
	default:
		return nil, fmt.Errorf("%w", errors.New(stmtType))
	}
}

// handleSelectStmt 处理Select语句
func (h *Handler) handleSelectStmt(ctx *pcontext.Context) (*plugin.Result, error) {
	var rows *sql.Rows
	var err error
	if tx := h.getTxByConnID(ctx.ConnID); tx != nil {
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

// getTxByConnID 根据客户端连接ID获取事务, 因为事务是与链接绑定的
func (h *Handler) getTxByConnID(connID uint32) datasource.Tx {
	if tx, ok := h.connID2Tx.Load(connID); ok {
		return tx
	}
	return nil
}

// handleCUDStmt 处理Insert、Update、Delete操作
func (h *Handler) handleCUDStmt(ctx *pcontext.Context) (*plugin.Result, error) {
	var err error
	var res sql.Result
	if tx := h.getTxByConnID(ctx.ConnID); tx != nil {
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

// handleStartTransactionStmt 处理开启事务语句
func (h *Handler) handleStartTransactionStmt(ctx *pcontext.Context) (*plugin.Result, error) {
	tx, err := h.ds.BeginTx(sharding.NewSingleTxContext(ctx), nil)
	if err != nil {
		return nil, err
	}
	h.connID2Tx.Store(ctx.ConnID, tx)
	return &plugin.Result{TxInTransaction: true}, nil
}

// handleCommitStmt 处理提交事务语句
func (h *Handler) handleCommitStmt(ctx *pcontext.Context) (*plugin.Result, error) {
	var err error
	tx := h.getTxByConnID(ctx.ConnID)
	if tx != nil {
		err = tx.Commit()
	}
	h.connID2Tx.Delete(ctx.ConnID)
	return &plugin.Result{}, err
}

// handleRollbackStmt 处理回滚事务语句
func (h *Handler) handleRollbackStmt(ctx *pcontext.Context) (*plugin.Result, error) {
	var err error
	tx := h.getTxByConnID(ctx.ConnID)
	if tx != nil {
		err = tx.Rollback()
	}
	h.connID2Tx.Delete(ctx.ConnID)
	return &plugin.Result{}, err
}
