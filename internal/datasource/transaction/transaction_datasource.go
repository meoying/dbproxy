package transaction

import (
	"context"
	"database/sql"

	"github.com/meoying/dbproxy/internal/datasource"
)

var _ datasource.DataSource = &txDataSourceWrapper{}

// txDataSourceWrapper 用于将datasource.Tx伪装成datasource.DataSource
type txDataSourceWrapper struct {
	tx datasource.Tx
}

func NewTransactionDataSource(tx datasource.Tx) datasource.DataSource {
	return &txDataSourceWrapper{tx: tx}
}

func (t *txDataSourceWrapper) Commit() error {
	return t.tx.Commit()
}

func (t *txDataSourceWrapper) Rollback() error {
	return t.tx.Rollback()
}

func (t *txDataSourceWrapper) BeginTx(ctx context.Context, opts *sql.TxOptions) (datasource.Tx, error) {
	panic("暂不支持,有需要可以提issue")
}

func (t *txDataSourceWrapper) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	return t.tx.Query(ctx, query)
}

func (t *txDataSourceWrapper) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	return t.tx.Exec(ctx, query)
}

func (t *txDataSourceWrapper) Close() error {
	panic("暂不支持,有需要可以提issue")
}
