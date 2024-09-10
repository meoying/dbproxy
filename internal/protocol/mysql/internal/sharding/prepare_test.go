package sharding

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
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
)

func TestShardingPrepare_BuildRealQuery(t *testing.T) {
	dbBase, tableBase := 2, 3
	dbPattern, tablePattern, dsPattern := "order_db_%d", "order_tab_%d", "0.db.cluster.company.com:3306"
	shardAlgorithm := &hash.Hash{
		ShardingKey:  "user_id",
		DBPattern:    &hash.Pattern{Name: dbPattern, Base: dbBase},
		TablePattern: &hash.Pattern{Name: tablePattern, Base: tableBase},
		DsPattern:    &hash.Pattern{Name: dsPattern, NotSharding: true},
	}

	m := map[string]*masterslave.MasterSlavesDB{
		"order_db_0": MasterSlavesMemoryDB(),
		"order_db_1": MasterSlavesMemoryDB(),
		"order_db_2": MasterSlavesMemoryDB(),
	}
	clusterDB := cluster.NewClusterDB(m)
	ds := map[string]datasource.DataSource{
		"0.db.cluster.company.com:3306": clusterDB,
	}
	dss := shardingsource.NewShardingDataSource(ds)

	testCases := []struct {
		name    string
		sql     string
		args    []any
		wantSql string
		wantErr error
	}{
		{
			name:    "select 没占位符",
			sql:     "SELECT `order_id`,`content` FROM order;",
			wantSql: "SELECT `order_id`,`content` FROM order ; ",
		},
		{
			name:    "select 1个占位符",
			sql:     "SELECT `user_id`,`order_id`,`content`,`account` FROM order WHERE `user_id`=?;",
			args:    []any{123},
			wantSql: "SELECT `user_id`,`order_id`,`content`,`account` FROM order WHERE `user_id` = 123 ; ",
		},
		{
			name:    "select 多个占位符",
			sql:     "SELECT `user_id`,`order_id`,`content`,`account` FROM order WHERE `user_id`=? AND `order_id` = ?;",
			args:    []any{123, 2},
			wantSql: "SELECT `user_id`,`order_id`,`content`,`account` FROM order WHERE `user_id` = 123 AND `order_id` = 2 ; ",
		},
		{
			name:    "select 占位符大于参数",
			sql:     "SELECT `user_id`,`order_id`,`content`,`account` FROM order  WHERE `user_id`=? AND `order_id` = ?;",
			args:    []any{123},
			wantErr: ErrPrepareArgsNoEqual,
		},
		{
			name:    "select 占位符小于参数",
			sql:     "SELECT `user_id`,`order_id`,`content`,`account` FROM order  WHERE `user_id`=?;",
			args:    []any{123, 2},
			wantErr: ErrPrepareArgsNoEqual,
		},
		{
			name:    "update 没占位符",
			sql:     "UPDATE order SET `user_id` = 1;",
			wantSql: "UPDATE order SET `user_id` = 1 ; ",
		},
		{
			name:    "update 1个占位符",
			sql:     "UPDATE order SET `user_id` = 1 WHERE `user_id`=?;",
			args:    []any{123},
			wantSql: "UPDATE order SET `user_id` = 1 WHERE `user_id` = 123 ; ",
		},
		{
			name:    "update 多个占位符",
			sql:     "UPDATE order SET `user_id` = 1 WHERE `user_id`=? AND `order_id`=?;",
			args:    []any{123, 2},
			wantSql: "UPDATE order SET `user_id` = 1 WHERE `user_id` = 123 AND `order_id` = 2 ; ",
		},
		{
			name:    "delete 没占位符",
			sql:     "DELETE FROM order;",
			wantSql: "DELETE FROM order ; ",
		},
		{
			name:    "delete 1个占位符",
			sql:     "DELETE FROM order WHERE `user_id`=?;",
			args:    []any{123},
			wantSql: "DELETE FROM order WHERE `user_id` = 123 ; ",
		},
		{
			name:    "delete 多个占位符",
			sql:     "DELETE FROM order WHERE `user_id`=? AND `order_id`=?;",
			args:    []any{123, 2},
			wantSql: "DELETE FROM order WHERE `user_id` = 123 AND `order_id` = 2 ; ",
		},
		{
			name:    "insert 没占位符",
			sql:     "INSERT INTO order (`user_id`) VALUES (123);",
			wantSql: "INSERT INTO order ( `user_id` ) VALUES ( 123 ) ; ",
		},
		{
			name:    "insert 1个占位符",
			sql:     "INSERT INTO order (`user_id`) VALUES (?);",
			args:    []any{123},
			wantSql: "INSERT INTO order ( `user_id` ) VALUES ( 123 ) ; ",
		},
		{
			name:    "insert 多个占位符",
			sql:     "INSERT INTO order (`user_id`, `order_id`) VALUES (?, ?);",
			args:    []any{123, 2},
			wantSql: "INSERT INTO order ( `user_id` , `order_id` ) VALUES ( 123,2 ) ; ",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &pcontext.Context{
				Context:     context.Background(),
				Query:       tc.sql,
				ParsedQuery: pcontext.NewParsedQuery(tc.sql, vparser.NewHintVisitor()),
			}
			handler, err := NewPrepareHandler(nil, shardAlgorithm, dss, ctx, tc.args)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			res, err := handler.ReplacePlaceholder()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantSql, res)
		})
	}
}

func TestShardingPrepare_Build(t *testing.T) {
	dbBase, tableBase := 2, 3
	dbPattern, tablePattern, dsPattern := "order_db_%d", "order_tab_%d", "0.db.cluster.company.com:3306"
	shardAlgorithm := &hash.Hash{
		ShardingKey:  "user_id",
		DBPattern:    &hash.Pattern{Name: dbPattern, Base: dbBase},
		TablePattern: &hash.Pattern{Name: tablePattern, Base: tableBase},
		DsPattern:    &hash.Pattern{Name: dsPattern, NotSharding: true},
	}

	m := map[string]*masterslave.MasterSlavesDB{
		"order_db_0": MasterSlavesMemoryDB(),
		"order_db_1": MasterSlavesMemoryDB(),
		"order_db_2": MasterSlavesMemoryDB(),
	}
	clusterDB := cluster.NewClusterDB(m)
	ds := map[string]datasource.DataSource{
		"0.db.cluster.company.com:3306": clusterDB,
	}
	dss := shardingsource.NewShardingDataSource(ds)

	testCases := []struct {
		name    string
		sql     string
		args    []any
		wantQs  []sharding.Query
		wantErr error
	}{
		{
			name: "select 没占位符",
			sql:  "SELECT `order_id`,`content` FROM order;",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "select 1个占位符",
			sql:  "SELECT `user_id`,`order_id`,`content`,`account` FROM order  WHERE `user_id`=?;",
			args: []any{123},
			wantQs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_0` WHERE `user_id` = ? ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
					Args:       []any{123},
				},
			},
		},
		{
			name: "select 多个占位符",
			sql:  "SELECT `user_id`,`order_id`,`content`,`account` FROM order  WHERE `user_id`=? AND `order_id` = ?;",
			args: []any{123, 2},
			wantQs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_0` WHERE `user_id` = ? AND `order_id` = ? ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
					Args:       []any{123, 2},
				},
			},
		},
		{
			name:    "select 占位符大于参数",
			sql:     "SELECT `user_id`,`order_id`,`content`,`account` FROM order  WHERE `user_id`=? AND `order_id` = ?;",
			args:    []any{123},
			wantErr: ErrPrepareArgsNoEqual,
		},
		{
			name:    "select 占位符小于参数",
			sql:     "SELECT `user_id`,`order_id`,`content`,`account` FROM order  WHERE `user_id`=?;",
			args:    []any{123, 2},
			wantErr: ErrPrepareArgsNoEqual,
		},
		{
			name: "update 没占位符",
			sql:  "UPDATE order SET `user_id` = 1;",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `user_id` = 1 ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "update 1个占位符",
			sql:  "UPDATE order SET `user_id` = 1 WHERE `user_id`=?;",
			args: []any{123},
			wantQs: []sharding.Query{
				{
					SQL:        "UPDATE `order_db_1`.`order_tab_0` SET `user_id` = 1 WHERE `user_id` = ? ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
					Args:       []any{123},
				},
			},
		},
		{
			name: "update 多个占位符",
			sql:  "UPDATE order SET `user_id` = 1 WHERE `user_id`=? AND `order_id`=?;",
			args: []any{123, 2},
			wantQs: []sharding.Query{
				{
					SQL:        "UPDATE `order_db_1`.`order_tab_0` SET `user_id` = 1 WHERE `user_id` = ? AND `order_id` = ? ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
					Args:       []any{123, 2},
				},
			},
		},
		{
			name: "delete 没占位符",
			sql:  "DELETE FROM order;",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "DELETE FROM `%s`.`%s` ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "delete 1个占位符",
			sql:  "DELETE FROM order WHERE `user_id`=?;",
			args: []any{123},
			wantQs: []sharding.Query{
				{
					SQL:        "DELETE FROM `order_db_1`.`order_tab_0` WHERE `user_id` = ? ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
					Args:       []any{123},
				},
			},
		},
		{
			name: "delete 多个占位符",
			sql:  "DELETE FROM order WHERE `user_id`=? AND `order_id`=?;",
			args: []any{123, 2},
			wantQs: []sharding.Query{
				{
					SQL:        "DELETE FROM `order_db_1`.`order_tab_0` WHERE `user_id` = ? AND `order_id` = ? ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
					Args:       []any{123, 2},
				},
			},
		},
		{
			name: "insert 没占位符",
			sql:  "INSERT INTO order (`user_id`) VALUES (123);",
			wantQs: []sharding.Query{
				{
					SQL:        "INSERT INTO `order_db_1`.`order_tab_0` ( `user_id` ) VALUES ( 123 ) ; ",
					DB:         "order_db_1",
					Table:      "order_tab_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "insert 1个占位符",
			sql:  "INSERT INTO order (`user_id`) VALUES (?);",
			args: []any{123},
			wantQs: []sharding.Query{
				{
					SQL:        "INSERT INTO `order_db_1`.`order_tab_0` ( `user_id` ) VALUES ( ? ) ; ",
					DB:         "order_db_1",
					Table:      "order_tab_0",
					Datasource: "0.db.cluster.company.com:3306",
					Args:       []any{123},
				},
			},
		},
		{
			name: "insert 多个占位符",
			sql:  "INSERT INTO order (`user_id`, `order_id`) VALUES (?, ?);",
			args: []any{123, 2},
			wantQs: []sharding.Query{
				{
					SQL:        "INSERT INTO `order_db_1`.`order_tab_0` ( `user_id` , `order_id` ) VALUES ( ?,? ) ; ",
					DB:         "order_db_1",
					Table:      "order_tab_0",
					Datasource: "0.db.cluster.company.com:3306",
					Args:       []any{123, 2},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &pcontext.Context{
				Context:     context.Background(),
				Query:       tc.sql,
				ParsedQuery: pcontext.NewParsedQuery(tc.sql, vparser.NewHintVisitor()),
			}
			handler, err := NewPrepareHandler(nil, shardAlgorithm, dss, ctx, tc.args)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			res, err := handler.Build(context.Background())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantQs, res)
		})
	}
}

type ShardingPrepareSuite struct {
	suite.Suite
	mock01   sqlmock.Sqlmock
	mockDB01 *sql.DB
	mock02   sqlmock.Sqlmock
	mockDB02 *sql.DB
}

func (s *ShardingPrepareSuite) SetupSuite() {
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

func (s *ShardingPrepareSuite) TearDownTest() {
	_ = s.mockDB01.Close()
	_ = s.mockDB02.Close()
}

func (s *ShardingPrepareSuite) TestShardingPrepare_QueryOrExec() {

	dbBase, tableBase := 2, 3
	dbPattern, tablePattern, dsPattern := "order_db_%d", "order_tab_%d", "0.db.cluster.company.com:3306"
	shardAlgorithm := &hash.Hash{
		ShardingKey:  "user_id",
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
		before           func()
		args             []any
		wantErr          error
		wantAffectedRows int64
		wantRes          []*OrderDetail
	}{
		{
			name: "insert",
			sql:  "INSERT INTO order (`user_id`,`order_id`,`content`,`account`) VALUES (?,?,?,?);",
			args: []any{1, 1, "1", 1.1},
			before: func() {
				s.mock02.ExpectPrepare(regexp.QuoteMeta("INSERT INTO `order_db_1`.`order_tab_1` ( `user_id` , `order_id` , `content` , `account` ) VALUES ( ?,?,?,? ) ; ")).ExpectExec().WillReturnResult(sqlmock.NewResult(1, 1))
			},
			wantAffectedRows: 1,
		},
		{
			name: "update",
			sql:  "UPDATE order SET `order_id` = 1 WHERE `user_id` = ?;",
			args: []any{1},
			before: func() {
				s.mock02.ExpectPrepare(regexp.QuoteMeta("UPDATE `order_db_1`.`order_tab_1` SET `order_id` = 1 WHERE `user_id` = ?")).ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
			},
			wantAffectedRows: 1,
		},
		{
			name: "delete",
			sql:  "DELETE FROM order WHERE `user_id` = ?;",
			args: []any{1},
			before: func() {
				s.mock02.ExpectPrepare(regexp.QuoteMeta("DELETE FROM `order_db_1`.`order_tab_1` WHERE `user_id` = ?")).ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
			},
			wantAffectedRows: 1,
		},
		{
			name: "select",
			sql:  "SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM order WHERE `user_id` = ?;",
			args: []any{1},
			before: func() {
				rows1 := s.mock01.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"})
				rows1.AddRow(234, 12, "Kevin", "Durant")
				s.mock02.ExpectPrepare(regexp.QuoteMeta("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_db_1`.`order_tab_1` WHERE `user_id` = ?  ; ")).ExpectQuery().WillReturnRows(rows1)
			},
			wantRes: []*OrderDetail{
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
			},
		},
	}
	for _, tc := range testcases {
		s.T().Run(tc.name, func(t *testing.T) {
			tc.before()
			ctx := &pcontext.Context{
				Context:     context.Background(),
				Query:       tc.sql,
				ParsedQuery: pcontext.NewParsedQuery(tc.sql, vparser.NewHintVisitor()),
			}
			stmt, err := dss.Prepare(ctx, sharding.Query{})
			require.NoError(t, err)
			handler, err := NewPrepareHandler(stmt, shardAlgorithm, dss, ctx, tc.args)
			assert.NoError(t, err)
			res, err := handler.QueryOrExec(context.Background())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			if res.Result != nil {
				affectRows, err := res.Result.RowsAffected()
				require.NoError(t, err)
				assert.Equal(t, tc.wantAffectedRows, affectRows)
			}
			if res.Rows != nil {
				data := make([]*OrderDetail, 0, 16)
				row := res.Rows
				for row.Next() {
					d := &OrderDetail{}
					err = row.Scan(&d.OrderId, &d.ItemId, &d.UsingCol1, &d.UsingCol2)
					require.NoError(t, err)
					data = append(data, d)
				}
				assert.ElementsMatch(t, tc.wantRes, data)
			}
		})
	}
}

func TestShardingPrepareSuite(t *testing.T) {
	suite.Run(t, &ShardingPrepareSuite{})
}
