package handler

import (
	"context"

	"github.com/ecodeclub/ekit/syncx"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/transaction"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

type baseHandler struct {
	ds        datasource.DataSource
	connID2Tx syncx.Map[uint32, datasource.Tx]
	newTxCtx  func(ctx context.Context) context.Context
}

func newBaseHandler(ds datasource.DataSource, name string) *baseHandler {
	return &baseHandler{
		ds: ds,
		newTxCtx: func(ctx context.Context) context.Context {
			return transaction.UsingTxType(ctx, name)
		},
	}
}

// getDatasource 获取本次执行需要使用的数据源
func (h *baseHandler) getDatasource(ctx *pcontext.Context) datasource.DataSource {
	if tx := h.getTxByConnID(ctx.ConnID); tx != nil {
		return transaction.NewTransactionDataSource(tx)
	}
	return h.ds
}

// getTxByConnID 根据客户端连接ID获取事务对象, 因为事务是与链接绑定的
func (h *baseHandler) getTxByConnID(connID uint32) datasource.Tx {
	if tx, ok := h.connID2Tx.Load(connID); ok {
		return tx
	}
	return nil
}

// getConnTransactionState 根据客户端连接ID获取链接的事务状态
func (h *baseHandler) getConnTransactionState(connID uint32) bool {
	// 有connID对应的Tx即表示对应的conn处于事务状态中
	_, ok := h.connID2Tx.Load(connID)
	return ok
}

// handleStartTransactionStmt 处理开启事务语句
func (h *baseHandler) handleStartTransactionStmt(ctx *pcontext.Context) (*plugin.Result, error) {
	tx, err := h.ds.BeginTx(h.newTxCtx(ctx), nil)
	if err != nil {
		return nil, err
	}
	h.connID2Tx.Store(ctx.ConnID, tx)
	return &plugin.Result{InTransaction: true}, nil
}

// handleCommitStmt 处理提交事务语句
func (h *baseHandler) handleCommitStmt(ctx *pcontext.Context) (*plugin.Result, error) {
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
func (h *baseHandler) handleRollbackStmt(ctx *pcontext.Context) (*plugin.Result, error) {
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
