package statement

import (
	"context"
	"testing"

	"github.com/meoying/dbproxy/internal/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type DelayStatementTestSuite struct {
	suite.Suite
}

func (c *DelayStatementTestSuite) TestClusterDbPrepare() {
	// 通过select不同的数据表示访问不同的db
	testCasesQuery := []struct {
		name         string
		ctx          context.Context
		query        query.Query
		before       func(ctx context.Context, query query.Query) (*MockClusterDataSource, *DelayStmt)
		after        func(ds *MockClusterDataSource)
		wantStmtsCnt int
		wantExistKey []string
	}{
		{
			name: "insert new stmt",
			ctx:  context.Background(),
			query: query.Query{
				SQL:        "SELECT `first_name` FROM `test_model`",
				DB:         "db_0",
				Table:      "test_model",
				Datasource: "1.db",
			},
			before: func(ctx context.Context, query query.Query) (*MockClusterDataSource, *DelayStmt) {
				ds, err := NewMockClusterDataSource()
				assert.NoError(c.T(), err)

				ds.mockDss["1.db"].ExpectPrepare("SELECT *").ExpectQuery().WillReturnRows()

				stmt, err := ds.Prepare(ctx, query)
				assert.NoError(c.T(), err)
				return ds, stmt.(*DelayStmt)
			},
			after: func(ds *MockClusterDataSource) {
				for _, v := range ds.dss {
					_ = v.db.Close()
				}
			},
			wantStmtsCnt: 1,
			wantExistKey: []string{"1.db.db_0.test_model"},
		},
		{
			name: "use exist stmt",
			ctx:  context.Background(),
			query: query.Query{
				SQL:        "SELECT `first_name` FROM `test_model`",
				DB:         "db_0",
				Table:      "test_model",
				Datasource: "1.db",
			},
			before: func(ctx context.Context, query query.Query) (*MockClusterDataSource, *DelayStmt) {
				ds, err := NewMockClusterDataSource()
				assert.NoError(c.T(), err)

				ds.mockDss["1.db"].ExpectPrepare("SELECT *").ExpectQuery().WillReturnRows()

				stmt, err := ds.Prepare(ctx, query)
				assert.NoError(c.T(), err)
				delayStmt := stmt.(*DelayStmt)
				delayStmt.stmts["1.db.db_0.test_model"], err = ds.dss["1.db"].Prepare(ctx, query)
				assert.NoError(c.T(), err)
				return ds, delayStmt
			},
			after: func(ds *MockClusterDataSource) {
				for _, v := range ds.dss {
					_ = v.db.Close()
				}
			},
			wantStmtsCnt: 1,
			wantExistKey: []string{"1.db.db_0.test_model"},
		},
		{
			name: "different ds",
			ctx:  context.Background(),
			query: query.Query{
				SQL:        "SELECT `first_name` FROM `test_model`",
				DB:         "db_0",
				Table:      "test_model",
				Datasource: "2.db",
			},
			before: func(ctx context.Context, query query.Query) (*MockClusterDataSource, *DelayStmt) {
				ds, err := NewMockClusterDataSource()
				assert.NoError(c.T(), err)

				ds.mockDss["1.db"].ExpectPrepare("SELECT *")
				ds.mockDss["2.db"].ExpectPrepare("SELECT *").ExpectQuery().WillReturnRows()

				stmt, err := ds.Prepare(ctx, query)
				assert.NoError(c.T(), err)
				delayStmt := stmt.(*DelayStmt)
				delayStmt.stmts["1.db.db_0.test_model"], err = ds.dss["1.db"].Prepare(ctx, query)
				assert.NoError(c.T(), err)
				return ds, delayStmt
			},
			after: func(ds *MockClusterDataSource) {
				for _, v := range ds.dss {
					_ = v.db.Close()
				}
			},
			wantStmtsCnt: 2,
			wantExistKey: []string{"1.db.db_0.test_model", "2.db.db_0.test_model"},
		},
		{
			name: "different db",
			ctx:  context.Background(),
			query: query.Query{
				SQL:        "SELECT `first_name` FROM `test_model`",
				DB:         "db_1",
				Table:      "test_model",
				Datasource: "1.db",
			},
			before: func(ctx context.Context, query query.Query) (*MockClusterDataSource, *DelayStmt) {
				ds, err := NewMockClusterDataSource()
				assert.NoError(c.T(), err)

				ds.mockDss["1.db"].ExpectPrepare("SELECT *")
				ds.mockDss["1.db"].ExpectPrepare("SELECT *").ExpectQuery().WillReturnRows()

				stmt, err := ds.Prepare(ctx, query)
				assert.NoError(c.T(), err)
				delayStmt := stmt.(*DelayStmt)
				delayStmt.stmts["1.db.db_0.test_model"], err = ds.dss["1.db"].Prepare(ctx, query)
				assert.NoError(c.T(), err)
				return ds, delayStmt
			},
			after: func(ds *MockClusterDataSource) {
				for _, v := range ds.dss {
					_ = v.db.Close()
				}
			},
			wantStmtsCnt: 2,
			wantExistKey: []string{"1.db.db_0.test_model", "1.db.db_1.test_model"},
		},
		{
			name: "different table",
			ctx:  context.Background(),
			query: query.Query{
				SQL:        "SELECT `first_name` FROM `test_model`",
				DB:         "db_0",
				Table:      "test_model_1",
				Datasource: "1.db",
			},
			before: func(ctx context.Context, query query.Query) (*MockClusterDataSource, *DelayStmt) {
				ds, err := NewMockClusterDataSource()
				assert.NoError(c.T(), err)

				ds.mockDss["1.db"].ExpectPrepare("SELECT *")
				ds.mockDss["1.db"].ExpectPrepare("SELECT *").ExpectQuery().WillReturnRows()

				stmt, err := ds.Prepare(ctx, query)
				assert.NoError(c.T(), err)
				delayStmt := stmt.(*DelayStmt)
				delayStmt.stmts["1.db.db_0.test_model"], err = ds.dss["1.db"].Prepare(ctx, query)
				assert.NoError(c.T(), err)
				return ds, delayStmt
			},
			after: func(ds *MockClusterDataSource) {
				for _, v := range ds.dss {
					_ = v.db.Close()
				}
			},
			wantStmtsCnt: 2,
			wantExistKey: []string{"1.db.db_0.test_model", "1.db.db_0.test_model_1"},
		},
	}

	for _, tc := range testCasesQuery {
		c.T().Run(tc.name, func(t *testing.T) {
			ds, stmt := tc.before(tc.ctx, tc.query)
			_, queryErr := stmt.Query(tc.ctx, tc.query)
			assert.NoError(t, queryErr)
			assert.Equal(t, tc.wantStmtsCnt, len(stmt.stmts))
			for _, key := range tc.wantExistKey {
				_, ok := stmt.stmts[key]
				assert.True(t, ok, "stmts里缺少对应的key值")
			}
			tc.after(ds)
		})
	}
}

func TestDelayStatementTestSuite(t *testing.T) {
	suite.Run(t, &DelayStatementTestSuite{})
}
