package sharding

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"github.com/meoying/dbproxy/internal/datasource/masterslave"

	"github.com/ecodeclub/ekit/slice"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/transaction"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	shardinghandler "github.com/meoying/dbproxy/internal/protocol/mysql/internal/sharding"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/vparser"
	"github.com/meoying/dbproxy/internal/sharding"
)

type connection struct {
	ds         datasource.DataSource
	exec       datasource.DataSource
	algorithm  sharding.Algorithm
	handlerMap map[string]shardinghandler.NewHandlerFunc
}

func newConnection(ds datasource.DataSource, algorithm sharding.Algorithm) *connection {
	return &connection{
		ds:        ds,
		exec:      ds,
		algorithm: algorithm,
		handlerMap: map[string]shardinghandler.NewHandlerFunc{
			vparser.SelectStmt: shardinghandler.NewSelectHandler,
			vparser.InsertStmt: shardinghandler.NewInsertBuilder,
			vparser.UpdateStmt: shardinghandler.NewUpdateHandler,
			vparser.DeleteStmt: shardinghandler.NewDeleteHandler,
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
		Context:     ctx,
		ParsedQuery: pcontext.NewParsedQuery(query),
		Query:       query,
		Args: slice.Map(args, func(idx int, src driver.NamedValue) any {
			return src
		}),
	}
	if pctx.ParsedQuery.UseMaster() {

		pctx.Context = masterslave.UseMaster(pctx.Context)
	}
	handler, err := c.getShardingHandler(pctx)
	if err != nil {
		return nil, err
	}
	return handler.QueryOrExec(pctx.Context)
}

func (c *connection) getShardingHandler(pctx *pcontext.Context) (shardinghandler.ShardingHandler, error) {
	newHandlerFunc, ok := c.handlerMap[pctx.ParsedQuery.Type()]
	if !ok {
		return nil, shardinghandler.ErrUnKnowSql
	}
	return newHandlerFunc(c.algorithm, c.exec, pctx)
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
	st, err := c.ds.Prepare(ctx, datasource.Query{SQL: query})
	if err != nil {
		return nil, err
	}
	return &stmt{
		conn:  c,
		stmt:  st,
		query: query,
	}, nil
}

func (c *connection) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.ds.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.IsolationLevel(opts.Isolation),
		ReadOnly:  opts.ReadOnly,
	})
	if err != nil {
		return nil, err
	}

	// 因用户调用的*sql.Tx上的Exec/Query方法最终会委派给创建该*sql.Tx的connection的Exec/Query方法
	// 并且要复用handlerMap中的SQL处理器所以需要将tx伪装成datasource以替换当前ds(原始ds)
	// 当该connection被复用时ResetSession方法会被调用并将ds还原为原始ds
	c.exec = transaction.NewTransactionDataSource(tx)
	return c.exec.(driver.Tx), nil
}

func (c *connection) Close() error {
	return nil
}

func (c *connection) ResetSession(ctx context.Context) error {
	c.exec = c.ds
	return nil
}

func (c *connection) IsValid() bool {
	return true
}

func (c *connection) CheckNamedValue(value *driver.NamedValue) error {
	return nil
}
