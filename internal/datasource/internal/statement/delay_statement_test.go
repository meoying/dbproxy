package statement

import (
	"context"
	"database/sql"
	"testing"

	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/stretchr/testify/assert"
)

type MockStmt struct{}

func (m *MockStmt) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	return nil, nil
}

func (m *MockStmt) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	return nil, nil
}

func (m *MockStmt) Close() error { return nil }

type MockFinder struct {
	FindTgtFunc func(ctx context.Context, query datasource.Query) (datasource.DataSource, error)
}

func (m *MockFinder) FindTgt(ctx context.Context, query datasource.Query) (datasource.DataSource, error) {
	if m.FindTgtFunc != nil {
		return m.FindTgtFunc(ctx, query)
	}
	return nil, nil
}

type MockDataSource struct{}

func (m *MockDataSource) BeginTx(ctx context.Context, opts *sql.TxOptions) (datasource.Tx, error) {
	return nil, nil
}

func (m *MockDataSource) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	return nil, nil
}

func (m *MockDataSource) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	return nil, nil
}

func (m *MockDataSource) Close() error {
	return nil
}

func (m *MockDataSource) Prepare(ctx context.Context, query datasource.Query) (datasource.Stmt, error) {
	return &MockStmt{}, nil
}

func TestDelayStmt_Query(t *testing.T) {
	ctx := context.Background()
	query := datasource.Query{
		SQL:        "SELECT `first_name` FROM `test_model`",
		DB:         "db_0",
		Datasource: "0.db.single.company.com:3306",
	}

	mockFinder := &MockFinder{
		FindTgtFunc: func(ctx context.Context, query datasource.Query) (datasource.DataSource, error) {
			return &MockDataSource{}, nil
		},
	}

	delayStmt := NewDelayStmt(mockFinder)

	rows, err := delayStmt.Query(ctx, query)
	assert.NoError(t, err)
	assert.Nil(t, rows)
}

func TestDelayStmt_Exec(t *testing.T) {
	ctx := context.Background()
	query := datasource.Query{
		SQL:        "INSERT INTO `test_model` (first_name) VALUES ('single')",
		DB:         "db_0",
		Datasource: "0.db.single.company.com:3306",
	}

	mockFinder := &MockFinder{
		FindTgtFunc: func(ctx context.Context, query datasource.Query) (datasource.DataSource, error) {
			return &MockDataSource{}, nil
		},
	}

	delayStmt := NewDelayStmt(mockFinder)

	result, err := delayStmt.Exec(ctx, query)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestDelayStmt_Close(t *testing.T) {
	mockFinder := &MockFinder{}

	delayStmt := NewDelayStmt(mockFinder)

	err := delayStmt.Close()
	assert.NoError(t, err)
}
