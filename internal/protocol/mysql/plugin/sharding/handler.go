package sharding

import (
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	shardinghandler "github.com/meoying/dbproxy/internal/protocol/mysql/internal/sharding"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/vparser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	"github.com/meoying/dbproxy/internal/sharding"
)

type Handler struct {
	ds         datasource.DataSource
	algorithm  sharding.Algorithm
	handlerMap map[string]shardinghandler.NewHandlerFunc
	// connID2Tx
}

func newHandler(ds datasource.DataSource, algorithm sharding.Algorithm) *Handler {
	return &Handler{
		ds:        ds,
		algorithm: algorithm,
		handlerMap: map[string]shardinghandler.NewHandlerFunc{
			vparser.SelectStmt: shardinghandler.NewSelectHandler,
			vparser.InsertStmt: shardinghandler.NewInsertBuilder,
			vparser.UpdateStmt: shardinghandler.NewUpdateHandler,
			vparser.DeleteStmt: shardinghandler.NewDeleteHandler,
			// 这里增加 BEGIN、COMMIT 和 ROLLBACK
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
	sqlName := checkVisitor.Visit(ctx.ParsedQuery.Root).(string)
	newHandlerFunc, ok := h.handlerMap[sqlName]
	if !ok {
		return nil, shardinghandler.ErrUnKnowSql
	}

	handler, err := newHandlerFunc(h.algorithm, h.ds, ctx)
	if err != nil {
		return nil, err
	}
	r, err := handler.QueryOrExec(ctx.Context)
	if err != nil {
		return nil, err
	}
	return (*plugin.Result)(r), nil
}
