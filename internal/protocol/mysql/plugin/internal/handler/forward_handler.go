package handler

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/ecodeclub/ekit/sqlx"
	"github.com/meoying/dbproxy/config/mysql/plugin/forward"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/masterslave"
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
	*baseHandler
	config forward.Config
}

func NewForwardHandler(ds datasource.DataSource, config forward.Config) *ForwardHandler {
	return &ForwardHandler{
		baseHandler: newBaseHandler(ds, transaction.Single),
		config:      config,
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
// TODO: 定义好Config后需要重新审查该方法
func (h *ForwardHandler) handleCRUDStmt(ctx *pcontext.Context, sqlTypeName string) (*plugin.Result, error) {
	var rows sqlx.Rows
	var res sql.Result
	var err error
	if sqlTypeName == vparser.SelectStmt {
		for _, hint := range ctx.ParsedQuery.Hints() {
			if strings.Contains(hint, "useMaster") {
				ctx.Context = masterslave.UseMaster(ctx.Context)
			}
		}
		rows, err = h.getDatasource(ctx).Query(ctx.Context, datasource.Query{
			SQL:  ctx.Query,
			Args: ctx.Args,
			// TODO: 如果时多主, 多从该如何选择db
			// TODO: DB字段和DataSource字段的区别?
			DB: h.config.DBName,
		})
	} else {
		res, err = h.getDatasource(ctx).Exec(ctx, datasource.Query{
			SQL:  ctx.Query,
			Args: ctx.Args,
			// TODO: 写操作默认走主库?
			DB: h.config.DBName,
		})
	}
	return &plugin.Result{
		Rows:               rows,
		Result:             res,
		InTransactionState: h.isInTransaction(ctx.ConnID),
	}, err
}
