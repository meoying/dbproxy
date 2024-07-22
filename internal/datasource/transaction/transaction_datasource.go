package transaction

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/meoying/dbproxy/internal/datasource"
)

var (
	_                       datasource.DataSource = &TxDatasource{}
	ErrUnSupportedOperation                       = errors.New("用Tx封装的DataSource暂不支持该操作")
)

// TxDatasource 用于将datasource.Tx伪装成datasource.DataSource
type TxDatasource struct {
	tx datasource.Tx
}

func NewTransactionDataSource(tx datasource.Tx) *TxDatasource {
	return &TxDatasource{tx: tx}
}

func (t *TxDatasource) Commit() error {
	return t.tx.Commit()
}

func (t *TxDatasource) Rollback() error {
	return t.tx.Rollback()
}

func (t *TxDatasource) BeginTx(ctx context.Context, opts *sql.TxOptions) (datasource.Tx, error) {
	return nil, fmt.Errorf("%w", ErrUnSupportedOperation)
}

func (t *TxDatasource) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	return t.tx.Query(ctx, query)
}

func (t *TxDatasource) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	return t.tx.Exec(ctx, query)
}

func (t *TxDatasource) Close() error {
	return fmt.Errorf("%w", ErrUnSupportedOperation)
}
