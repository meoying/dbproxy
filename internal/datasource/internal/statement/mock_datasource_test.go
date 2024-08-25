package statement

import (
	"context"
	"database/sql"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/internal/errs"
)

type MockClusterDataSource struct {
	mockDss map[string]sqlmock.Sqlmock
	dss     map[string]*MockSingleDataSource
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

func NewMockClusterDataSource() (*MockClusterDataSource, error) {
	mockMaster1DB, mockMaster, err := sqlmock.New()
	if err != nil {
		return nil, err
	}

	mockMaster2DB, mockMaster2, err := sqlmock.New()
	if err != nil {
		return nil, err
	}

	return &MockClusterDataSource{
		mockDss: map[string]sqlmock.Sqlmock{
			"1.db": mockMaster,
			"2.db": mockMaster2,
		},
		dss: map[string]*MockSingleDataSource{
			"1.db": NewMockSingleDataSource(mockMaster1DB),
			"2.db": NewMockSingleDataSource(mockMaster2DB),
		},
	}, nil
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
