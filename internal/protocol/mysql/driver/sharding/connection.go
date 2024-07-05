package sharding

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/ecodeclub/ekit/slice"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/transaction"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	shardinghandler "github.com/meoying/dbproxy/internal/protocol/mysql/internal/sharding"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/vparser"
	"github.com/meoying/dbproxy/internal/sharding"
)

type connection struct {
	ds         datasource.DataSource
	origin     datasource.DataSource
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
func (c *connection) Ping(_ context.Context) error {
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
	handler, err := c.getShardingHandler(pctx, query, args)
	if err != nil {
		return nil, err
	}
	return handler.QueryOrExec(pctx.Context)
}

func (c *connection) getShardingHandler(pctx *pcontext.Context, query string, args []driver.NamedValue) (shardinghandler.ShardingHandler, error) {
	checkVisitor := vparser.NewCheckVisitor()
	sqlName := checkVisitor.Visit(pctx.ParsedQuery.Root).(string)
	newHandlerFunc, ok := c.handlerMap[sqlName]
	if !ok {
		return nil, shardinghandler.ErrUnKnowSql
	}
	return newHandlerFunc(c.algorithm, c.ds, pctx)
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
	return c.PrepareContext(context.Background(), query)
}

func (c *connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &stmt{}, nil
}

func (c *connection) Begin() (driver.Tx, error) {
	panic("暂不支持,请使用BeginTx")
}

func (c *connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.ds.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.IsolationLevel(opts.Isolation),
		ReadOnly:  opts.ReadOnly,
	})
	if err != nil {
		return nil, err
	}
	if c.origin == nil {
		c.origin = c.ds
	}
	// 因用户在tx上的Exec/Query调用最终会回到当前Conn上
	// 所以需要将tx伪装成datasource并替换原始ds
	// 当当前Conn被重用时调用ResetSession将ds还原为原始ds
	c.ds = transaction.NewTransactionDataSource(tx)
	return c.ds.(driver.Tx), nil
}

func (c *connection) Close() error {
	return nil
}

func (c *connection) ResetSession(ctx context.Context) error {
	if c.origin != nil {
		// 已创建过tx且当前ds是tx伪装的,需要还原回newConnection时传入的原始ds
		c.ds = c.origin
		c.origin = nil
	}
	return nil
}

func (c *connection) IsValid() bool {
	return true
}

func (c *connection) CheckNamedValue(value *driver.NamedValue) error {
	return nil
}
