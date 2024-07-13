package datasource

import (
	"context"
	"database/sql"

	"github.com/meoying/dbproxy/internal/query"
)

type Executor interface {
	Query(ctx context.Context, query Query) (*sql.Rows, error)
	Exec(ctx context.Context, query Query) (sql.Result, error)
}

type TxBeginner interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error)
}

type Finder interface {
	FindTgt(ctx context.Context, query Query) (TxBeginner, error)
}

type Tx interface {
	Executor
	Commit() error
	Rollback() error
}

type DataSource interface {
	TxBeginner
	Executor
	// TODO 添加driver.Pinger接口中的ping方法
	Prepare(ctx context.Context, query Query) (Stmt, error)
	Close() error
}

type Stmt interface {
	Executor
	Close() error
}

type Query = query.Query
