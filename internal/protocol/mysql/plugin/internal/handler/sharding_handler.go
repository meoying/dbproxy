package handler

import (
	"errors"
	"fmt"
	"github.com/meoying/dbproxy/internal/datasource/masterslave"

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
	hintMap := ctx.ParsedQuery.Hints()
	v,ok := hintMap["useMaster"]
	if ok && v.Value.(bool) {
		ctx.Context = masterslave.UseMaster(ctx.Context)
	}
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
