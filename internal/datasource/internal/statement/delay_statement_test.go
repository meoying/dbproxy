package statement

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/sharding"
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
	c.mockMaster.ExpectPrepare("SELECT *").ExpectQuery().WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("cluster1 master"))
	c.mockMaster.ExpectPrepare("SELECT *").ExpectQuery().WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("cluster1 master1"))
	c.mockMaster2.ExpectPrepare("SELECT *").ExpectQuery().WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("cluster2 master"))
	c.mockMaster2.ExpectPrepare("SELECT *").ExpectQuery().WillReturnRows(sqlmock.NewRows([]string{"mark"}).AddRow("cluster2 master1"))

	testCasesQuery := []struct {
		name     string
		ctx      context.Context
		query    sharding.Query
		wantResp []string
		wantErr  error
	}{
		{
			name: "cluster1 prepare use delay",
			ctx:  context.Background(),
			query: sharding.Query{
				SQL:        "SELECT `first_name` FROM `test_model`",
				DB:         "db_0",
				Table:      "test_model",
				Datasource: "1.db",
			},
			wantResp: []string{"cluster1 master", "cluster1 master1"},
		},
		{
			name: "cluster2 prepare use delay",
			ctx:  context.Background(),
			query: sharding.Query{
				SQL:        "SELECT `first_name` FROM `test_model`",
				DB:         "db_0",
				Table:      "test_model",
				Datasource: "2.db",
			},
			wantResp: []string{"cluster2 master", "cluster2 master1"},
		},
	}

	for _, tc := range testCasesQuery {
		c.T().Run(tc.name, func(t *testing.T) {
			var resp []string
			stmt, err := c.DataSource.Prepare(tc.ctx, tc.query)
			assert.NoError(t, err)
			assert.IsType(t, &DelayStmt{}, stmt)
			rows, queryErr := stmt.Query(tc.ctx, tc.query)
			assert.Equal(t, queryErr, tc.wantErr)
			if queryErr != nil {
				return
			}
			assert.NotNil(t, rows)
			ok := rows.Next()
			assert.True(t, ok)

			val := new(string)
			err = rows.Scan(val)
			assert.Nil(t, err)
			if err != nil {
				return
			}
			assert.NotNil(t, val)

			resp = append(resp, *val)

			delayStmt := stmt.(*DelayStmt)
			key := tc.query.Datasource + "." + tc.query.DB + "." + tc.query.Table
			_, exists := delayStmt.stmts[key]
			assert.True(t, exists, "没有找到对应stmts中的数据")

			rows, queryErr = stmt.Query(tc.ctx, tc.query)
			assert.Equal(t, queryErr, tc.wantErr)
			if queryErr != nil {
				return
			}
			assert.NotNil(t, rows)
			ok = rows.Next()
			assert.True(t, ok)

			val = new(string)
			err = rows.Scan(val)
			assert.Nil(t, err)
			if err != nil {
				return
			}
			assert.NotNil(t, val)

			resp = append(resp, *val)
			assert.ElementsMatch(t, tc.wantResp, resp)
		})
	}
}

func TestMockDataSourceSuite(t *testing.T) {
	suite.Run(t, &MockDataSourceSuite{})
}
