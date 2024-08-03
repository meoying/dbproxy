package handler

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/ecodeclub/ekit/sqlx"
	"github.com/ecodeclub/ekit/syncx"
	"github.com/meoying/dbproxy/config/mysql/plugins/forward"
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
	stmtID2Stmt       syncx.Map[uint32, datasource.Stmt]
	stmtID2PrepareCtx syncx.Map[uint32, *pcontext.Context]
	config            forward.Config
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
	case vparser.PrepareStmt:
		return h.handlePrepareStmt(ctx)
	case vparser.ExecutePrepareStmt:
		return h.handleExecutePrepareStmt(ctx)
	case vparser.DeallocatePrepareStmt:
		return h.handleDeallocatePrepareStmt(ctx)
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

func (h *ForwardHandler) handlePrepareStmt(ctx *pcontext.Context) (*plugin.Result, error) {
	stmt, err := h.getStmtPreparer(ctx).Prepare(ctx, datasource.Query{
		SQL: ctx.Query,
		DB:  h.config.DBName,
	})
	if err != nil {
		return nil, err
	}
	h.stmtID2Stmt.Store(ctx.StmtID, stmt)
	h.stmtID2PrepareCtx.Store(ctx.StmtID, &pcontext.Context{
		Context: ctx.Context,
		// SELECT * FROM order where `user_id` = ?;
		// SELECT * FROM order where `user_id` = '?';
		ParsedQuery: pcontext.NewParsedQuery(h.convertQuery(ctx.Query), nil),
		Query:       ctx.Query,
		ConnID:      ctx.ConnID,
		StmtID:      ctx.StmtID,
	})
	return &plugin.Result{
		InTransactionState: h.isInTransaction(ctx.ConnID),
		StmtID:             ctx.StmtID,
	}, nil
}

func (h *ForwardHandler) getStmtByStmtID(stmtID uint32) (datasource.Stmt, error) {
	if stmt, ok := h.stmtID2Stmt.Load(stmtID); ok {
		return stmt, nil
	}
	return nil, fmt.Errorf("未找到id为%d的stmt", stmtID)
}

func (h *ForwardHandler) getPrepareContextByStmtID(stmtID uint32) (*pcontext.Context, error) {
	if ctx, ok := h.stmtID2PrepareCtx.Load(stmtID); ok {
		return ctx, nil
	}
	return nil, fmt.Errorf("未找到id为%d的pcontext.Context", stmtID)
}

func (h *ForwardHandler) handleExecutePrepareStmt(ctx *pcontext.Context) (*plugin.Result, error) {
	// ctx.Args应该是传递过来的参数列表
	stmt, err := h.getStmtByStmtID(ctx.StmtID)
	if err != nil {
		return nil, err
	}
	c, err := h.getPrepareContextByStmtID(ctx.StmtID)
	if err != nil {
		return nil, err
	}
	log.Printf("handleExecutePrepareStmt: type = %#v, query = %#v, args = %#v", c.ParsedQuery.Type(), c.Query, ctx.Args)
	var result sql.Result
	var rows sqlx.Rows
	switch c.ParsedQuery.Type() {
	case vparser.SelectStmt:
		rows, err = stmt.Query(ctx.Context, datasource.Query{
			SQL:  c.Query,
			Args: ctx.Args,
			DB:   h.config.DBName,
		})
	case vparser.InsertStmt, vparser.UpdateStmt, vparser.DeleteStmt:
		result, err = stmt.Exec(ctx.Context, datasource.Query{
			SQL:  c.Query,
			Args: ctx.Args,
			DB:   h.config.DBName,
		})
	}
	log.Printf("handleExecutePrepareStmt: result : %#v, rows : %#v\n", result, rows)
	return &plugin.Result{
		Result:             result,
		Rows:               rows,
		InTransactionState: h.isInTransaction(ctx.ConnID),
		StmtID:             ctx.StmtID,
	}, err
}

func (h *ForwardHandler) handleDeallocatePrepareStmt(ctx *pcontext.Context) (*plugin.Result, error) {
	stmt, err := h.getStmtByStmtID(ctx.StmtID)
	if err != nil {
		return nil, err
	}
	err = stmt.Close()
	h.stmtID2Stmt.Delete(ctx.StmtID)
	h.stmtID2PrepareCtx.Delete(ctx.StmtID)
	return &plugin.Result{
		InTransactionState: h.isInTransaction(ctx.ConnID),
	}, err
}
