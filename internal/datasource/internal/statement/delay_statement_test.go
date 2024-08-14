package statement

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type MockDataSourceSuite struct {
	suite.Suite
	datasource.DataSource
	mockMaster1DB *sql.DB
	mockMaster    sqlmock.Sqlmock

	mockMaster2DB *sql.DB
	mockMaster2   sqlmock.Sqlmock
}

func (c *MockDataSourceSuite) SetupTest() {
	t := c.T()
	c.initMock(t)
}

func (c *MockDataSourceSuite) TearDownTest() {
	_ = c.mockMaster1DB.Close()

	_ = c.mockMaster2DB.Close()
}

func (c *MockDataSourceSuite) initMock(t *testing.T) {
	var err error
	c.mockMaster1DB, c.mockMaster, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}

	c.mockMaster2DB, c.mockMaster2, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}

	c.DataSource = NewMockClusterDataSource(map[string]*MockSingleDataSource{
		"1.db": NewMockSingleDataSource(c.mockMaster1DB),
		"2.db": NewMockSingleDataSource(c.mockMaster2DB),
	})

}

func (c *MockDataSourceSuite) TestClusterDbPrepare() {
	// 通过select不同的数据表示访问不同的db
	testCasesQuery := []struct {
		initMockSql  func()
		name         string
		ctx          context.Context
		query        query.Query
		before       func(ctx context.Context, query query.Query) *DelayStmt
		wantStmtsCnt int
		wantExistKey []string
	}{
		{
			initMockSql: func() {
				c.mockMaster.ExpectPrepare("SELECT *").ExpectQuery().WillReturnRows()
			},
			name: "insert new stmt",
			ctx:  context.Background(),
			query: query.Query{
				SQL:        "SELECT `first_name` FROM `test_model`",
				DB:         "db_0",
				Table:      "test_model",
				Datasource: "1.db",
			},
			before: func(ctx context.Context, query query.Query) *DelayStmt {
				stmt, err := c.DataSource.Prepare(ctx, query)
				assert.NoError(c.T(), err)
				return stmt.(*DelayStmt)
			},
			wantStmtsCnt: 1,
			wantExistKey: []string{"1.db.db_0.test_model"},
		},
		{
			initMockSql: func() {
				c.mockMaster.ExpectPrepare("SELECT *").ExpectQuery().WillReturnRows()
			},
			name: "use exist stmt",
			ctx:  context.Background(),
			query: query.Query{
				SQL:        "SELECT `first_name` FROM `test_model`",
				DB:         "db_0",
				Table:      "test_model",
				Datasource: "1.db",
			},
			before: func(ctx context.Context, query query.Query) *DelayStmt {
				stmt, err := c.DataSource.Prepare(ctx, query)
				assert.NoError(c.T(), err)
				delayStmt := stmt.(*DelayStmt)
				delayStmt.stmts["1.db.db_0.test_model"], err = c.DataSource.(*MockClusterDataSource).dss["1.db"].Prepare(ctx, query)
				assert.NoError(c.T(), err)
				return delayStmt
			},
			wantStmtsCnt: 1,
			wantExistKey: []string{"1.db.db_0.test_model"},
		},
		{
			initMockSql: func() {
				c.mockMaster.ExpectPrepare("SELECT *")
				c.mockMaster2.ExpectPrepare("SELECT *").ExpectQuery().WillReturnRows()
			},
			name: "different ds",
			ctx:  context.Background(),
			query: query.Query{
				SQL:        "SELECT `first_name` FROM `test_model`",
				DB:         "db_0",
				Table:      "test_model",
				Datasource: "2.db",
			},
			before: func(ctx context.Context, query query.Query) *DelayStmt {
				stmt, err := c.DataSource.Prepare(ctx, query)
				assert.NoError(c.T(), err)
				delayStmt := stmt.(*DelayStmt)
				delayStmt.stmts["1.db.db_0.test_model"], err = c.DataSource.(*MockClusterDataSource).dss["1.db"].Prepare(ctx, query)
				assert.NoError(c.T(), err)
				return delayStmt
			},
			wantStmtsCnt: 2,
			wantExistKey: []string{"1.db.db_0.test_model", "2.db.db_0.test_model"},
		},
		{
			initMockSql: func() {
				c.mockMaster.ExpectPrepare("SELECT *")
				c.mockMaster.ExpectPrepare("SELECT *").ExpectQuery().WillReturnRows()
			},
			name: "different db",
			ctx:  context.Background(),
			query: query.Query{
				SQL:        "SELECT `first_name` FROM `test_model`",
				DB:         "db_1",
				Table:      "test_model",
				Datasource: "1.db",
			},
			before: func(ctx context.Context, query query.Query) *DelayStmt {
				stmt, err := c.DataSource.Prepare(ctx, query)
				assert.NoError(c.T(), err)
				delayStmt := stmt.(*DelayStmt)
				delayStmt.stmts["1.db.db_0.test_model"], err = c.DataSource.(*MockClusterDataSource).dss["1.db"].Prepare(ctx, query)
				assert.NoError(c.T(), err)
				return delayStmt
			},
			wantStmtsCnt: 2,
			wantExistKey: []string{"1.db.db_0.test_model", "1.db.db_1.test_model"},
		},
		{
			initMockSql: func() {
				c.mockMaster.ExpectPrepare("SELECT *")
				c.mockMaster.ExpectPrepare("SELECT *").ExpectQuery().WillReturnRows()
			},
			name: "different table",
			ctx:  context.Background(),
			query: query.Query{
				SQL:        "SELECT `first_name` FROM `test_model`",
				DB:         "db_0",
				Table:      "test_model_1",
				Datasource: "1.db",
			},
			before: func(ctx context.Context, query query.Query) *DelayStmt {
				stmt, err := c.DataSource.Prepare(ctx, query)
				assert.NoError(c.T(), err)
				delayStmt := stmt.(*DelayStmt)
				delayStmt.stmts["1.db.db_0.test_model"], err = c.DataSource.(*MockClusterDataSource).dss["1.db"].Prepare(ctx, query)
				assert.NoError(c.T(), err)
				return delayStmt
			},
			wantStmtsCnt: 2,
			wantExistKey: []string{"1.db.db_0.test_model", "1.db.db_0.test_model_1"},
		},
	}

	for _, tc := range testCasesQuery {
		c.T().Run(tc.name, func(t *testing.T) {
			tc.initMockSql()
			stmt := tc.before(tc.ctx, tc.query)
			_, queryErr := stmt.Query(tc.ctx, tc.query)
			assert.NoError(t, queryErr)
			assert.Equal(t, len(stmt.stmts), tc.wantStmtsCnt)
			for _, key := range tc.wantExistKey {
				_, ok := stmt.stmts[key]
				assert.True(t, ok, "stmts里缺少对应的key值")
			}
		})
	}
}

func TestMockDataSourceSuite(t *testing.T) {
	suite.Run(t, &MockDataSourceSuite{})
}
