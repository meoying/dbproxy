package sharding

import (
	"context"
	"database/sql/driver"

	"github.com/ecodeclub/ekit/slice"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	shardinghandler "github.com/meoying/dbproxy/internal/protocol/mysql/internal/sharding"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/vparser"
	"github.com/meoying/dbproxy/internal/sharding"
)

type connection struct {
	ds         datasource.DataSource
	algorithm  sharding.Algorithm
	handlerMap map[string]shardinghandler.NewHandlerFunc
}

func newConnection(ds datasource.DataSource, algorithm sharding.Algorithm) *connection {
	return &connection{
		ds:        ds,
		algorithm: algorithm,
		handlerMap: map[string]shardinghandler.NewHandlerFunc{
			vparser.SelectSql: shardinghandler.NewSelectHandler,
			vparser.InsertSql: shardinghandler.NewInsertBuilder,
			vparser.UpdateSql: shardinghandler.NewUpdateHandler,
			vparser.DeleteSql: shardinghandler.NewDeleteHandler,
		},
	}
}

// Ping
// TODO: 需要委派给ds来检查连通性
func (c *connection) Ping(ctx context.Context) error {
	panic("暂不支持,有需要可以提issue")
}

func (c *connection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	res, err := c.queryOrExec(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return &result{
		r: res.Result,
	}, nil
}

func (c *connection) queryOrExec(ctx context.Context, query string, args []driver.NamedValue) (*shardinghandler.Result, error) {
	pctx := &pcontext.Context{
		Context: ctx,
		ParsedQuery: pcontext.ParsedQuery{
			Root: ast.Parse(query),
		},
		Query: query,
		Args: slice.Map(args, func(idx int, src driver.NamedValue) any {
			return src
		}),
		InTransition: false,
	}
	checkVisitor := vparser.NewCheckVisitor()
	sqlName := checkVisitor.Visit(pctx.ParsedQuery.Root).(string)
	newHandlerFunc, ok := c.handlerMap[sqlName]
	if !ok {
		return nil, shardinghandler.ErrUnKnowSql
	}
	handler, err := newHandlerFunc(c.algorithm, c.ds, pctx)
	if err != nil {
		return nil, err
	}
	return handler.QueryOrExec(pctx.Context)
}

func (c *connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	res, err := c.queryOrExec(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return &rows{
		sqlxRows: res.Rows,
	}, nil
}

func (c *connection) Prepare(query string) (driver.Stmt, error) {
	return &stmt{}, nil
}

func (c *connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &stmt{}, nil
}

func (c *connection) Begin() (driver.Tx, error) {
	return &transaction{}, nil
}

func (c *connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return &transaction{}, nil
}

func (c *connection) Close() error {
	return nil
}

func (c *connection) ResetSession(ctx context.Context) error {
	return nil
}

func (c *connection) IsValid() bool {
	return true
}

func (c *connection) CheckNamedValue(value *driver.NamedValue) error {
	return nil
}
