package sharding

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/mattn/go-sqlite3"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/cluster"
	"github.com/meoying/dbproxy/internal/datasource/masterslave"
	"github.com/meoying/dbproxy/internal/datasource/shardingsource"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/vparser"
	"github.com/meoying/dbproxy/internal/sharding"
	"github.com/meoying/dbproxy/internal/sharding/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/multierr"
)

func newMockErr(dbName string) error {
	return fmt.Errorf("mock error for %s", dbName)
}
func TestShardingInsert_Build(t *testing.T) {
	dbBase, tableBase, dsBase := 2, 3, 2
	dbPattern, tablePattern, dsPattern := "order_db_%d", "order_tab_%d", "%d.db.cluster.company.com:3306"
	shardAlgorithm := &hash.Hash{
		ShardingKey:  "uid",
		DBPattern:    &hash.Pattern{Name: dbPattern, Base: dbBase},
		TablePattern: &hash.Pattern{Name: tablePattern, Base: tableBase},
		DsPattern:    &hash.Pattern{Name: dsPattern, Base: dsBase},
	}
	m := map[string]*masterslave.MasterSlavesDB{
		"order_db_0": MasterSlavesMemoryDB(),
		"order_db_1": MasterSlavesMemoryDB(),
		"order_db_2": MasterSlavesMemoryDB(),
	}
	clusterDB := cluster.NewClusterDB(m)
	ds := map[string]datasource.DataSource{
		"0.db.cluster.company.com:3306": clusterDB,
		"1.db.cluster.company.com:3306": clusterDB,
	}

	dss := shardingsource.NewShardingDataSource(ds)

	testCases := []struct {
		name    string
		sql     string
		wantQs  []sharding.Query
		wantErr error
	}{
		{
			name: "插入一条数据",
			sql:  "INSERT INTO order (`uid`,`order_id`,`content`,`account`) VALUES (1,3,'content',1.1);",
			wantQs: []sharding.Query{
				{
					SQL:        "INSERT INTO `order_db_1`.`order_tab_1` ( `uid` , `order_id` , `content` , `account` ) VALUES ( 1,3,'content',1.1 ) ; ",
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "插入多个元素",
			sql:  "INSERT INTO order (`uid`,`order_id`,`content`,`account`) VALUES ( 1,1,'1',1.0 ) , ( 2,2,'2',2.0 ),( 3,3,'3',3.0 ),( 4,4,'4',4.0 );",
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("INSERT INTO %s.%s ( `uid` , `order_id` , `content` , `account` ) VALUES ( 4,4,'4',4.0 ) ; ", "`order_db_0`", "`order_tab_1`"),
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("INSERT INTO %s.%s ( `uid` , `order_id` , `content` , `account` ) VALUES ( 2,2,'2',2.0 ) ; ", "`order_db_0`", "`order_tab_2`"),
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("INSERT INTO %s.%s ( `uid` , `order_id` , `content` , `account` ) VALUES ( 3,3,'3',3.0 ) ; ", "`order_db_1`", "`order_tab_0`"),
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("INSERT INTO %s.%s ( `uid` , `order_id` , `content` , `account` ) VALUES ( 1,1,'1',1.0 ) ; ", "`order_db_1`", "`order_tab_1`"),
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "插入多个元素, 但是不同的元素会被分配到同一个库",
			sql:  "INSERT INTO order (`uid`,`order_id`,`content`,`account`) VALUES(1,1,'1',1.0),(7,7,'7',7.0);",
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("INSERT INTO %s.%s ( `uid` , `order_id` , `content` , `account` ) VALUES ( 1,1,'1',1.0 ) , ( 7,7,'7',7.0 ) ; ", "`order_db_1`", "`order_tab_1`"),
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "插入多个元素, 有不同的元素会被分配到同一个库表",
			sql:  "INSERT INTO order (`uid`,`order_id`,`content`,`account`) VALUES(1,1,'1',1.0),(7,7,'7',7.0),(2,2,'2',2.0),(8,8,'8',8.0),(3,3,'3',3.0);",
			wantQs: []sharding.Query{

				{
					SQL:        fmt.Sprintf("INSERT INTO %s.%s ( `uid` , `order_id` , `content` , `account` ) VALUES ( 2,2,'2',2.0 ) , ( 8,8,'8',8.0 ) ; ", "`order_db_0`", "`order_tab_2`"),
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("INSERT INTO %s.%s ( `uid` , `order_id` , `content` , `account` ) VALUES ( 3,3,'3',3.0 ) ; ", "`order_db_1`", "`order_tab_0`"),
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("INSERT INTO %s.%s ( `uid` , `order_id` , `content` , `account` ) VALUES ( 1,1,'1',1.0 ) , ( 7,7,'7',7.0 ) ; ", "`order_db_1`", "`order_tab_1`"),
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name:    "插入时，插入的列没有包含分库分表的列",
			sql:     "INSERT INTO order (`usid`,`order_id`,`content`,`account`) VALUES (1,1,'zlw',1.1)",
			wantErr: ErrInsertShardingKeyNotFound,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &pcontext.Context{
				Context:     context.Background(),
				Query:       tc.sql,
				ParsedQuery: pcontext.NewParsedQuery(tc.sql, vparser.NewHintVisitor()),
			}
			handler, err := NewInsertBuilder(shardAlgorithm, dss, ctx)
			require.NoError(t, err)
			res, err := handler.Build(context.Background())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantQs, res)
		})
	}
}

type ShardingInsertSuite struct {
	suite.Suite
	mock01   sqlmock.Sqlmock
	mockDB01 *sql.DB
	mock02   sqlmock.Sqlmock
	mockDB02 *sql.DB
}

func (s *ShardingInsertSuite) SetupSuite() {
	t := s.T()
	var err error
	s.mockDB01, s.mock01, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	s.mockDB02, s.mock02, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}

}

func (s *ShardingInsertSuite) TearDownTest() {
	_ = s.mockDB01.Close()
	_ = s.mockDB02.Close()
}

func (s *ShardingInsertSuite) TestShardingInsert_Exec() {

	dbBase, tableBase := 2, 3
	dbPattern, tablePattern, dsPattern := "order_db_%d", "order_tab_%d", "0.db.cluster.company.com:3306"
	shardAlgorithm := &hash.Hash{
		ShardingKey:  "uid",
		DBPattern:    &hash.Pattern{Name: dbPattern, Base: dbBase},
		TablePattern: &hash.Pattern{Name: tablePattern, Base: tableBase},
		DsPattern:    &hash.Pattern{Name: dsPattern, NotSharding: true},
	}

	m := map[string]*masterslave.MasterSlavesDB{
		"order_db_0": MasterSlavesMockDB(s.mockDB01),
		"order_db_1": MasterSlavesMockDB(s.mockDB02),
	}
	clusterDB := cluster.NewClusterDB(m)
	ds := map[string]datasource.DataSource{
		"0.db.cluster.company.com:3306": clusterDB,
	}
	dss := shardingsource.NewShardingDataSource(ds)
	testcases := []struct {
		name             string
		sql              string
		mockDb           func()
		wantErr          error
		wantAffectedRows int64
	}{
		{
			name: "跨表插入全部成功",
			sql:  "INSERT INTO order (`uid`,`order_id`,`content`,`account`) VALUES (1,1,'1',1.0),(2,2,'2',2.0),(3,3,'3',3.0);",
			mockDb: func() {
				s.mock02.MatchExpectationsInOrder(false)
				s.mock02.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_db_1`.`order_tab_1` ( `uid` , `order_id` , `content` , `account` ) VALUES ( 1,1,'1',1.0 ) ; ")).WillReturnResult(sqlmock.NewResult(1, 1))
				s.mock02.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_db_1`.`order_tab_0` ( `uid` , `order_id` , `content` , `account` ) VALUES ( 3,3,'3',3.0 ) ; ")).WillReturnResult(sqlmock.NewResult(3, 1))
				s.mock01.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_db_0`.`order_tab_2` ( `uid` , `order_id` , `content` , `account` ) VALUES ( 2,2,'2',2.0 ) ; ")).WillReturnResult(sqlmock.NewResult(2, 1))
			},
			wantAffectedRows: 3,
		},
		{
			name: "部分插入失败",
			sql:  "INSERT INTO order (`uid`,`order_id`,`content`,`account`) VALUES (1,1,'1',1.0),(2,2,'2',2.0),(3,3,'3',3.0);",
			mockDb: func() {
				s.mock02.MatchExpectationsInOrder(false)
				s.mock02.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_db_1`.`order_tab_1` ( `uid` , `order_id` , `content` , `account` ) VALUES ( 1,1,'1',1.0 ) ; ")).WillReturnError(newMockErr("db01"))
				s.mock02.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_db_1`.`order_tab_0` ( `uid` , `order_id` , `content` , `account` ) VALUES ( 3,3,'3',3.0 ) ; ")).WillReturnResult(sqlmock.NewResult(1, 1))
				s.mock01.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_db_0`.`order_tab_2` ( `uid` , `order_id` , `content` , `account` ) VALUES ( 2,2,'2',2.0 ) ; ")).WillReturnResult(sqlmock.NewResult(1, 1))
			},
			wantErr: multierr.Combine(newMockErr("db01")),
		},
		{
			name: "全部插入失败",
			sql:  "INSERT INTO order (`uid`,`order_id`,`content`,`account`) VALUES (1,1,'1',1.0),(2,2,'2',2.0),(3,3,'3',3.0);",
			mockDb: func() {
				s.mock02.MatchExpectationsInOrder(false)
				s.mock02.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_db_1`.`order_tab_1` ( `uid` , `order_id` , `content` , `account` ) VALUES ( 1,1,'1',1.0 ) ; ")).WillReturnError(newMockErr("db"))
				s.mock02.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_db_1`.`order_tab_0` ( `uid` , `order_id` , `content` , `account` ) VALUES ( 3,3,'3',3.0 ) ; ")).WillReturnError(newMockErr("db"))
				s.mock01.ExpectExec(regexp.QuoteMeta("INSERT INTO `order_db_0`.`order_tab_2` ( `uid` , `order_id` , `content` , `account` ) VALUES ( 2,2,'2',2.0 ) ; ")).WillReturnError(newMockErr("db"))
			},
			wantErr: multierr.Combine(newMockErr("db"), newMockErr("db"), newMockErr("db")),
		},
	}
	for _, tc := range testcases {
		s.T().Run(tc.name, func(t *testing.T) {
			tc.mockDb()
			ctx := &pcontext.Context{
				Context:     context.Background(),
				Query:       tc.sql,
				ParsedQuery: pcontext.NewParsedQuery(tc.sql, vparser.NewHintVisitor()),
			}
			handler, err := NewInsertBuilder(shardAlgorithm, dss, ctx)
			require.NoError(t, err)
			res, err := handler.QueryOrExec(context.Background())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			affectRows, err := res.Result.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, tc.wantAffectedRows, affectRows)
		})
	}
}

func TestShardingInsertSuite(t *testing.T) {
	suite.Run(t, &ShardingInsertSuite{})
}
