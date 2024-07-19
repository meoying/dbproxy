package sharding

import (
	"errors"
	"fmt"

	"github.com/ecodeclub/ekit/syncx"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/transaction"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	shardinghandler "github.com/meoying/dbproxy/internal/protocol/mysql/internal/sharding"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/vparser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	"github.com/meoying/dbproxy/internal/sharding"
)

type Handler struct {
	ds           datasource.DataSource
	algorithm    sharding.Algorithm
	crudHandlers map[string]shardinghandler.NewHandlerFunc
	connID2Tx    syncx.Map[uint32, datasource.Tx]
}

func newHandler(ds datasource.DataSource, algorithm sharding.Algorithm) *Handler {
	return &Handler{
		ds:        ds,
		algorithm: algorithm,
		crudHandlers: map[string]shardinghandler.NewHandlerFunc{
			vparser.SelectStmt: shardinghandler.NewSelectHandler,
			vparser.InsertStmt: shardinghandler.NewInsertBuilder,
			vparser.UpdateStmt: shardinghandler.NewUpdateHandler,
			vparser.DeleteStmt: shardinghandler.NewDeleteHandler,
		},
	}
}

func (h *Handler) Handle(ctx *pcontext.Context) (*plugin.Result, error) {
	// 要完成几个步骤：
	// 1. 从 ctx.ParsedQuery 里面拿到 Where 部分，参考 ast 里面的东西来看怎么拿 WHERE
	// 如果是 INSERT，则是拿到 VALUE 或者 VALUES 的部分
	// 2. 用 1 步骤的结果，调用 p.algorithm 拿到分库分表的结果
	// 3. 调用 p.ds.Exec 或者 p.ds.Query
	checkVisitor := vparser.NewCheckVisitor()
	sqlTypeName := checkVisitor.Visit(ctx.ParsedQuery.Root).(string)
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

// handleCRUDStmt 处理Select、Insert、Update、Delete语句
func (h *Handler) handleCRUDStmt(ctx *pcontext.Context, sqlName string) (*plugin.Result, error) {
	newHandlerFunc, ok := h.crudHandlers[sqlName]
	if !ok {
		return nil, shardinghandler.ErrUnKnowSql
	}
	handler, err := newHandlerFunc(h.algorithm, h.getDatasource(ctx), ctx)
	if err != nil {
		return nil, err
	}
	r, err := handler.QueryOrExec(ctx.Context)
	if err != nil {
		return nil, err
	}
	r.InTransaction = h.getConnTransactionState(ctx.ConnID)
	return (*plugin.Result)(r), nil
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
	tx, err := h.ds.BeginTx(transaction.UsingTxType(ctx, transaction.Delay), nil)
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
