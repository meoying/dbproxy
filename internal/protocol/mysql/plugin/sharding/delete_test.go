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
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	pcontext "github.com/meoying/dbproxy/internal/protocol/mysql/plugin/context"
	"github.com/meoying/dbproxy/internal/sharding"
	"github.com/meoying/dbproxy/internal/sharding/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestDeleteHandler_Build(t *testing.T) {
	dbBase, tableBase := 2, 3
	orderDBPattern, orderTablePattern := "order_db_%d", "order_tab_%d"
	dsPattern := "0.db.cluster.company.com:3306"
	shardAlgorithm := &hash.Hash{
		ShardingKey:  "user_id",
		DBPattern:    &hash.Pattern{Name: orderDBPattern, Base: dbBase},
		TablePattern: &hash.Pattern{Name: orderTablePattern, Base: tableBase},
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
		wantQs  []sharding.Query
		wantErr error
	}{
		{
			name: "eq",
			sql:  "DELETE  FROM order WHERE `user_id`=1;",
			wantQs: []sharding.Query{
				{
					SQL:        "DELETE FROM `order_db_1`.`order_tab_1` WHERE `user_id` = 1 ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "not have where",
			sql:  "DELETE  FROM order;",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "DELETE FROM `%s`.`%s` ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
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
			name: "where or",
			sql:  "DELETE FROM order WHERE `user_id`=123 OR `user_id`=234;",
			wantQs: []sharding.Query{
				{
					SQL:        "DELETE FROM `order_db_1`.`order_tab_0` WHERE `user_id` = 123 OR `user_id` = 234 ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "DELETE FROM `order_db_0`.`order_tab_0` WHERE `user_id` = 123 OR `user_id` = 234 ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or broadcast",
			sql:  "DELETE FROM ORDER WHERE `user_id`=123 OR `order_id`=2;",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "DELETE FROM `%s`.`%s` WHERE `user_id` = 123 OR `order_id` = 2 ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
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
			name:   "where and empty",
			sql:    "delete from order where user_id=123 and user_id=234;",
			wantQs: []sharding.Query{},
		},
		{
			name: "where and or",
			sql:  "DELETE FROM order WHERE (`user_id` = 123 AND `order_id`=12) OR `user_id`=234;",
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("DELETE FROM %s.%s WHERE ( `user_id` = 123 AND `order_id` = 12 ) OR `user_id` = 234 ; ", "`order_db_1`", "`order_tab_0`"),
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("DELETE FROM %s.%s WHERE ( `user_id` = 123 AND `order_id` = 12 ) OR `user_id` = 234 ; ", "`order_db_0`", "`order_tab_0`"),
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and",
			sql:  "DELETE FROM order WHERE `user_id`=123 OR (`user_id`=181 AND `user_id`=234);",
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("DELETE FROM %s.%s WHERE `user_id` = 123 OR ( `user_id` = 181 AND `user_id` = 234 ) ; ", "`order_db_1`", "`order_tab_0`"),
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where lt",
			sql:  "DELETE FROM order WHERE `user_id` < 123 ;",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "DELETE FROM `%s`.`%s` WHERE `user_id` < 123 ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
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
			name: "where eq and lt or gt",
			sql:  "DELETE FROM ORDER WHERE (`user_id`=12 AND `user_id`<133) OR `user_id`>234;",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "DELETE FROM `%s`.`%s` WHERE ( `user_id` = 12 AND `user_id` < 133 ) OR `user_id` > 234 ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
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
			name: "where in",
			sql:  "DELETE FROM order WHERE `user_id` IN (12,35,101);",
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("DELETE FROM %s.%s WHERE `user_id` IN ( 12 , 35 , 101 ) ; ", "`order_db_1`", "`order_tab_2`"),
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("DELETE FROM %s.%s WHERE `user_id` IN ( 12 , 35 , 101 ) ; ", "`order_db_0`", "`order_tab_0`"),
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in or eq",
			sql:  "DELETE  FROM order WHERE `user_id` IN (12,35,101) OR `user_id` = 531;",
			wantQs: []sharding.Query{
				{
					SQL:        "DELETE FROM `order_db_1`.`order_tab_2` WHERE `user_id` IN ( 12 , 35 , 101 ) OR `user_id` = 531 ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "DELETE FROM `order_db_1`.`order_tab_0` WHERE `user_id` IN ( 12 , 35 , 101 ) OR `user_id` = 531 ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "DELETE FROM `order_db_0`.`order_tab_0` WHERE `user_id` IN ( 12 , 35 , 101 ) OR `user_id` = 531 ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not in",
			sql:  "DELETE FROM order WHERE `user_id` NOT IN (12,35,101);",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "DELETE FROM `%s`.`%s` WHERE `user_id` NOT IN ( 12 , 35 , 101 ) ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
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
			name: "where not gt",
			sql:  "DELETE FROM order WHERE NOT (`user_id` > 101);",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "DELETE FROM `%s`.`%s` WHERE NOT ( `user_id` > 101 ) ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
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
			name: "where not eq",
			sql:  "DELETE FROM order WHERE NOT (`user_id`=101);",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "DELETE FROM `%s`.`%s` WHERE NOT ( `user_id` = 101 ) ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
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
			name: "where not neq",
			sql:  "DELETE FROM order WHERE NOT (user_id != 101);",
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("DELETE FROM %s.%s WHERE NOT ( user_id != 101 ) ; ", "`order_db_1`", "`order_tab_2`"),
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &pcontext.Context{
				Context: context.Background(),
				Query:   tc.sql,
				ParsedQuery: pcontext.ParsedQuery{
					Root: ast.Parse(tc.sql),
				},
			}
			handler, err := NewDeleteHandler(shardAlgorithm, dss, ctx)
			require.NoError(t, err)
			res, err := handler.Build(context.Background())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.ElementsMatch(t, tc.wantQs, res)
		})
	}
}

type DeleteHandlerSuite struct {
	suite.Suite
	mock01   sqlmock.Sqlmock
	mockDB01 *sql.DB
	mock02   sqlmock.Sqlmock
	mockDB02 *sql.DB
}

func (d *DeleteHandlerSuite) SetupSuite() {
	t := d.T()
	var err error
	d.mockDB01, d.mock01, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	d.mockDB02, d.mock02, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}

}

func (d *DeleteHandlerSuite) TestDeleteHandler_Exec() {
	dbBase, tableBase := 2, 3
	dbPattern, tablePattern, dsPattern := "order_db_%d", "order_tab_%d", "0.db.cluster.company.com:3306"
	shardAlgorithm := &hash.Hash{
		ShardingKey:  "user_id",
		DBPattern:    &hash.Pattern{Name: dbPattern, Base: dbBase},
		TablePattern: &hash.Pattern{Name: tablePattern, Base: tableBase},
		DsPattern:    &hash.Pattern{Name: dsPattern, NotSharding: true},
	}
	m := map[string]*masterslave.MasterSlavesDB{
		"order_db_0": MasterSlavesMockDB(d.mockDB01),
		"order_db_1": MasterSlavesMockDB(d.mockDB02),
	}
	clusterDB := cluster.NewClusterDB(m)
	ds := map[string]datasource.DataSource{
		"0.db.cluster.company.com:3306": clusterDB,
	}
	dss := shardingsource.NewShardingDataSource(ds)
	testCases := []struct {
		name             string
		sql              string
		mockDB           func()
		wantAffectedRows int64
		wantErr          error
	}{
		{
			name: "where eq",
			sql:  "DELETE FROM order WHERE `user_id`=1;",
			mockDB: func() {
				d.mock02.ExpectExec(regexp.QuoteMeta("DELETE FROM `order_db_1`.`order_tab_1` WHERE `user_id` = 1 ; ")).WillReturnResult(sqlmock.NewResult(1, 1))
			},
			wantAffectedRows: 1,
		},
		{
			name: "where or",
			sql:  "DELETE FROM order WHERE `user_id`=123 OR `user_id`=234;",
			mockDB: func() {
				d.mock02.ExpectExec(regexp.QuoteMeta("DELETE FROM `order_db_1`.`order_tab_0` WHERE `user_id` = 123 OR `user_id` = 234 ; ")).WillReturnResult(sqlmock.NewResult(1, 2))
				d.mock01.ExpectExec(regexp.QuoteMeta("DELETE FROM `order_db_0`.`order_tab_0` WHERE `user_id` = 123 OR `user_id` = 234 ; ")).WillReturnResult(sqlmock.NewResult(1, 2))
			},
			wantAffectedRows: 4,
		},
	}
	for _, tc := range testCases {
		d.T().Run(tc.name, func(t *testing.T) {
			tc.mockDB()
			ctx := &pcontext.Context{
				Context: context.Background(),
				Query:   tc.sql,
				ParsedQuery: pcontext.ParsedQuery{
					Root: ast.Parse(tc.sql),
				},
			}
			handler, err := NewDeleteHandler(shardAlgorithm, dss, ctx)
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

func TestDeleteHandlerSuite(t *testing.T) {
	suite.Run(t, &DeleteHandlerSuite{})
}
