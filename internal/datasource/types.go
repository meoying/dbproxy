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

type Stmt interface {
	Executor
	Close() error
}

type StmtPreparer interface {
	Prepare(ctx context.Context, query Query) (Stmt, error)
}

type TxBeginner interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error)
}

type Finder interface {
	FindTgt(ctx context.Context, query Query) (DataSource, error)
}

type Tx interface {
	StmtPreparer
	Executor
	Commit() error
	Rollback() error
}

type DataSource interface {
	TxBeginner
	StmtPreparer
	Executor
	// TODO 添加driver.Pinger接口中的ping方法
	Close() error
}

type Query = query.Query
