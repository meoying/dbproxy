package statement

import (
	"context"
	"database/sql"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/internal/errs"
)

type MockClusterDataSource struct {
	dss map[string]*MockSingleDataSource
}

func (m *MockClusterDataSource) FindTgt(ctx context.Context, query datasource.Query) (datasource.DataSource, error) {
	ds, ok := m.dss[query.Datasource]
	if !ok {
		return nil, errs.NewErrNotFoundTargetDB(query.DB)
	}
	return ds, nil
}

func (m *MockClusterDataSource) BeginTx(ctx context.Context, opts *sql.TxOptions) (datasource.Tx, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockClusterDataSource) Prepare(ctx context.Context, query datasource.Query) (datasource.Stmt, error) {
	return Prepare(ctx, m)
}

func (m *MockClusterDataSource) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockClusterDataSource) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockClusterDataSource) Close() error {
	//TODO implement me
	panic("implement me")
}

func NewMockClusterDataSource(dss map[string]*MockSingleDataSource) *MockClusterDataSource {
	return &MockClusterDataSource{
		dss: dss,
	}
}

type MockSingleDataSource struct {
	db *sql.DB
}

func (m *MockSingleDataSource) BeginTx(ctx context.Context, opts *sql.TxOptions) (datasource.Tx, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockSingleDataSource) Prepare(ctx context.Context, query datasource.Query) (datasource.Stmt, error) {
	stmt, err := m.db.PrepareContext(ctx, query.SQL)
	return NewPreparedStatement(stmt), err
}

func (m *MockSingleDataSource) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockSingleDataSource) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockSingleDataSource) Close() error {
	//TODO implement me
	panic("implement me")
}

func NewMockSingleDataSource(db *sql.DB) *MockSingleDataSource {
	return &MockSingleDataSource{
		db: db,
	}
}
