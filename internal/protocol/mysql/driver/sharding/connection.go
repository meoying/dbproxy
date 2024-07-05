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
	handler, err := c.getShardingHandler(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return handler.QueryOrExec(ctx)
}

func (c *connection) getShardingHandler(ctx context.Context, query string, args []driver.NamedValue) (shardinghandler.ShardingHandler, error) {
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
	// 默认使用DelayTx
	return c.BeginTx(NewDelayTxContext(context.Background()), driver.TxOptions{
		Isolation: driver.IsolationLevel(sql.LevelDefault),
	})
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
	// 使用tx伪装成datasource并替换初始化时候的ds
	// 这样在当前conn上执行的SQL底层都是走tx
	c.ds = transaction.NewTransactionDataSource(tx)
	return c.ds.(driver.Tx), nil
}

func (c *connection) Close() error {
	return nil
}

func (c *connection) ResetSession(ctx context.Context) error {
	if c.origin != nil {
		// 此时表明创建过tx,需要ds还原回newConnection时传入传入的ds
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
