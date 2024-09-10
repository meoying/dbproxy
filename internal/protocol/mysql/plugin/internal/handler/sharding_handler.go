package handler

import (
	"errors"
	"fmt"
	"log"

	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/transaction"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	shardinghandler "github.com/meoying/dbproxy/internal/protocol/mysql/internal/sharding"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/vparser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	"github.com/meoying/dbproxy/internal/sharding"
)

type ShardingHandler struct {
	*baseHandler
	algorithm    sharding.Algorithm
	stmtHandlers map[string]shardinghandler.NewHandlerFunc
}

func NewShardingHandler(ds datasource.DataSource, algorithm sharding.Algorithm) *ShardingHandler {
	return &ShardingHandler{
		baseHandler: newBaseHandler(ds, transaction.Delay),
		algorithm:   algorithm,
		stmtHandlers: map[string]shardinghandler.NewHandlerFunc{
			vparser.SelectStmt: shardinghandler.NewSelectHandler,
			vparser.InsertStmt: shardinghandler.NewInsertBuilder,
			vparser.UpdateStmt: shardinghandler.NewUpdateHandler,
			vparser.DeleteStmt: shardinghandler.NewDeleteHandler,
		},
	}
}

func (h *ShardingHandler) Handle(ctx *pcontext.Context) (*plugin.Result, error) {
	// 要完成几个步骤：
	// 1. 从 ctx.ParsedQuery 里面拿到 Where 部分，参考 ast 里面的东西来看怎么拿 WHERE
	// 如果是 INSERT，则是拿到 VALUE 或者 VALUES 的部分
	// 2. 用 1 步骤的结果，调用 p.algorithm 拿到分库分表的结果
	// 3. 调用 p.ds.Exec 或者 p.ds.Query
	sqlTypeName := ctx.ParsedQuery.Type()
	switch sqlTypeName {
	case vparser.SelectStmt, vparser.InsertStmt, vparser.UpdateStmt, vparser.DeleteStmt:
		return h.handleCRUDStmt(ctx, sqlTypeName)
	case vparser.PrepareStmt:
		return h.handlePrepareStmt(ctx, datasource.Query{
			SQL: ctx.Query,
		})
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
		return nil, fmt.Errorf("尚未支持的SQL特性: %w", errors.New(sqlTypeName))
	}
}

// handleCRUDStmt 处理Select、Insert、Update、Delete语句
func (h *ShardingHandler) handleCRUDStmt(ctx *pcontext.Context, sqlName string) (*plugin.Result, error) {
	newStmtHandler, ok := h.stmtHandlers[sqlName]
	if !ok {
		return nil, shardinghandler.ErrUnKnowSql
	}
	stmtHandler, err := newStmtHandler(h.algorithm, h.getDatasource(ctx), ctx)
	if err != nil {
		return nil, err
	}
	r, err := stmtHandler.QueryOrExec(ctx.Context)
	if err != nil {
		return nil, err
	}
	r.InTransactionState = h.isInTransaction(ctx.ConnID)
	return (*plugin.Result)(r), nil
}

func (h *ShardingHandler) handleExecutePrepareStmt(ctx *pcontext.Context) (*plugin.Result, error) {
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

	prepareHandler, err := shardinghandler.NewPrepareHandler(stmt, h.algorithm, h.getDatasource(ctx), c, ctx.Args)
	if err != nil {
		return nil, err
	}

	r, err := prepareHandler.QueryOrExec(ctx.Context)
	if err != nil {
		return nil, err
	}
	log.Printf("handleExecutePrepareStmt: result : %#v, rows : %#v\n", r.Result, r.Rows)

	r.InTransactionState = h.isInTransaction(ctx.ConnID)
	return (*plugin.Result)(r), nil
}
