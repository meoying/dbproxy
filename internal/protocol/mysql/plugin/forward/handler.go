package forward

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/ecodeclub/ekit/sqlx"
	"github.com/ecodeclub/ekit/syncx"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/transaction"
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
	sqlTypeName := visitor.Visit(ctx.ParsedQuery.Root).(string)
	switch sqlTypeName {
	case vparser.SelectStmt, vparser.InsertStmt, vparser.UpdateStmt, vparser.DeleteStmt:
		return h.handleCRUDStmt(ctx, sqlTypeName)
	case vparser.StartTransactionStmt:
		return h.handleStartTransactionStmt(ctx)
	case vparser.CommitStmt:
		return h.handleCommitStmt(ctx)
	case vparser.RollbackStmt:
		return h.handleRollbackStmt(ctx)
	default:
		return nil, fmt.Errorf("%w", errors.New(sqlTypeName))
	}
}

// handleCRUDStmt 处理Select、Insert、Update、Delete操作
func (h *Handler) handleCRUDStmt(ctx *pcontext.Context, sqlTypeName string) (*plugin.Result, error) {
	var rows sqlx.Rows
	var res sql.Result
	var err error
	if sqlTypeName == vparser.SelectStmt {
		rows, err = h.getDatasource(ctx).Query(ctx, datasource.Query{
			SQL:  ctx.Query,
			Args: ctx.Args,
		})
	} else {
		res, err = h.getDatasource(ctx).Exec(ctx, datasource.Query{
			SQL:  ctx.Query,
			Args: ctx.Args,
		})
	}
	return &plugin.Result{
		Rows:          rows,
		Result:        res,
		InTransaction: h.getConnTransactionState(ctx.ConnID),
	}, err
}

// getDatasource 获取本次执行需要使用的数据源
func (h *Handler) getDatasource(ctx *pcontext.Context) datasource.DataSource {
	if tx := h.getTxByConnID(ctx.ConnID); tx != nil {
		return transaction.NewTransactionDataSource(tx)
	}
	return h.ds
}

// getTxByConnID 根据客户端连接ID获取事务对象, 因为事务是与链接绑定的
func (h *Handler) getTxByConnID(connID uint32) datasource.Tx {
	if tx, ok := h.connID2Tx.Load(connID); ok {
		return tx
	}
	return nil
}

// getConnTransactionState 根据客户端连接ID获取链接的事务状态
func (h *Handler) getConnTransactionState(connID uint32) bool {
	_, ok := h.connID2Tx.Load(connID)
	return ok
}

// handleStartTransactionStmt 处理开启事务语句
func (h *Handler) handleStartTransactionStmt(ctx *pcontext.Context) (*plugin.Result, error) {
	tx, err := h.ds.BeginTx(transaction.UsingTxType(ctx, transaction.Single), nil)
	if err != nil {
		return nil, err
	}
	h.connID2Tx.Store(ctx.ConnID, tx)
	return &plugin.Result{InTransaction: true}, nil
}

// handleCommitStmt 处理提交事务语句
func (h *Handler) handleCommitStmt(ctx *pcontext.Context) (*plugin.Result, error) {
	var err error
	tx := h.getTxByConnID(ctx.ConnID)
	if tx != nil {
		err = tx.Commit()
	}
	if err == nil {
		h.connID2Tx.Delete(ctx.ConnID)
	}
	return &plugin.Result{}, err
}

// handleRollbackStmt 处理回滚事务语句
func (h *Handler) handleRollbackStmt(ctx *pcontext.Context) (*plugin.Result, error) {
	var err error
	tx := h.getTxByConnID(ctx.ConnID)
	if tx != nil {
		err = tx.Rollback()
	}
	if err == nil {
		h.connID2Tx.Delete(ctx.ConnID)
	}
	return &plugin.Result{}, err
}
