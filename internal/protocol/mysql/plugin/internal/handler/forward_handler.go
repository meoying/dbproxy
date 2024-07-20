package handler

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/ecodeclub/ekit/sqlx"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/transaction"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/vparser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

// ForwardHandler 什么也不做，就是转发请求
// 一般用于测试环境
// 这个实现有一个巨大的问题，即 ForwardHandler 不是线程安全的
// TODO 后续要考虑多个事务（不同的 Connection) 同时执行的问题
type ForwardHandler struct {
	*BaseHandler
}

func NewForwardHandler(ds datasource.DataSource) *ForwardHandler {
	return &ForwardHandler{
		BaseHandler: NewBaseHandler(ds, transaction.Single),
	}
}

func (h *ForwardHandler) Handle(ctx *pcontext.Context) (*plugin.Result, error) {
	sqlTypeName := ctx.ParsedQuery.Type()
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
func (h *ForwardHandler) handleCRUDStmt(ctx *pcontext.Context, sqlTypeName string) (*plugin.Result, error) {
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
