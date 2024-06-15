package sharding

import (
	"context"
	"database/sql"
	"fmt"
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
	"regexp"
	"testing"
)

func TestUpdateHandler_Build(t *testing.T) {
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
			name: "where eq",
			sql:  "update order set order_id=1,content='1',account=1.0 where user_id = 1;",
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `order_id`=?,`content`=?,`account`=? WHERE `user_id`=?;", "`order_db_1`", "`order_tab_1`"),
					Args:       []any{1, "1", 1.0, 1},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "not where",
			sql:  "UPDATE order SET `content`='1',`account`=1.0;",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=?;"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0},
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
			sql:  "UPDATE order  SET `content`='1',`account`=1.0 WHERE (`user_id`=123) OR (`user_id`=234);",
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content`=?,`account`=? WHERE (`user_id`=?) OR (`user_id`=?);", "`order_db_1`", "`order_tab_0`"),
					Args:       []any{"1", 1.0, 123, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content`=?,`account`=? WHERE (`user_id`=?) OR (`user_id`=?);", "`order_db_0`", "`order_tab_0`"),
					Args:       []any{"1", 1.0, 123, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or broadcast",
			sql:  "UPDATE order SET `content`='1',`account`=1.0 WHERE (`user_id`=123) OR (`order_id`=2);",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE (`user_id`=?) OR (`order_id`=?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 123, 2},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where and or",
			sql:  "UPDATE order SET `content`='1',`account`=1.0 WHERE ((`user_id`=123) AND (`order_id`=12)) OR (`user_id`=234);",
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content`=?,`account`=? WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);", "`order_db_1`", "`order_tab_0`"),
					Args:       []any{"1", 1.0, 123, 12, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content`=?,`account`=? WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);", "`order_db_0`", "`order_tab_0`"),
					Args:       []any{"1", 1.0, 123, 12, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and",
			sql:  "UPDATE order SET `content`='1',`account`=1.0 WHERE (`user_id`=123) OR ((`user_id`=181) AND (`user_id`=234));",
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content`=?,`account`=? WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));", "`order_db_1`", "`order_tab_0`"),
					Args:       []any{"1", 1.0, 123, 181, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where lt",
			sql:  "UPDATE order SET `content`='1',`account`=1.0 WHERE `user_id`<123;",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE `user_id`<?;"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 123},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where lt eq",
			sql:  "UPDATE order SET `content`='1',`account`=1.0 WHERE `user_id`<=123;",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE `user_id`<=?;"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 123},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where gt",
			sql:  "UPDATE order  SET `content`='1',`account`=1.0 WHERE `user_id`>123;",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE `user_id`>?;"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 123},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where gt eq",
			sql:  "UPDATE order SET `content`='1',`account`=1.0 WHERE `user_id`>=123;",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE `user_id`>=?;"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 123},
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
			sql:  "UPDATE order SET `content`='1',`account`=1.0 WHERE ((`user_id`=12) AND (`user_id`<133)) OR (`user_id`>234);",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE ((`user_id`=?) AND (`user_id`<?)) OR (`user_id`>?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 12, 133, 234},
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
			sql:  "UPDATE order SET `content`='1',`account`=1.0 WHERE `user_id` IN (12,35,101);",
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content`=?,`account`=? WHERE `user_id` IN (?,?,?);", "`order_db_1`", "`order_tab_2`"),
					Args:       []any{"1", 1.0, 12, 35, 101},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content`=?,`account`=? WHERE `user_id` IN (?,?,?);", "`order_db_0`", "`order_tab_0`"),
					Args:       []any{"1", 1.0, 12, 35, 101},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in and eq",
			sql:  "UPDATE order  SET `content`='1',`account`=1.0 WHERE (`user_id` IN (12,35,101)) AND (`user_id`=234);",
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content`=?,`account`=? WHERE (`user_id` IN (?,?,?)) AND (`user_id`=?);", "`order_db_0`", "`order_tab_0`"),
					Args:       []any{"1", 1.0, 12, 35, 101, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in or eq",
			sql:  "UPDATE order SET `content`='1',`account`=1.0 WHERE (`user_id` IN (12,35,101)) OR (`user_id`=531);",
			wantQs: []sharding.Query{
				{
					SQL:        "UPDATE `order_db_1`.`order_tab_2` SET `content`=?,`account`=? WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{"1", 1.0, 12, 35, 101, 531},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "UPDATE `order_db_1`.`order_tab_0` SET `content`=?,`account`=? WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{"1", 1.0, 12, 35, 101, 531},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "UPDATE `order_db_0`.`order_tab_0` SET `content`=?,`account`=? WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{"1", 1.0, 12, 35, 101, 531},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not in",
			sql:  "UPDATE order SET `content`='1',`account`=1.0 WHERE `user_id` NOT IN (12,35,101);",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE `user_id` NOT IN (?,?,?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 12, 35, 101},
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
			sql:  "UPDATE order SET `content`='1',`account`=1.0 WHERE NOT (`user_id`>101);",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE NOT (`user_id`>?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 101},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where not lt",
			sql:  "UPDATE order SET `content`='1',`account`=1.0 WHERE NOT (`user_id`<101);",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE NOT (`user_id`<?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 101},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where not gt eq",
			sql:  "UPDATE order SET `content`='1',`account`=1.0 WHERE NOT (`user_id`>=101);",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE NOT (`user_id`>=?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 101},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where not lt eq",
			sql:  "UPDATE order SET `content`='1',`account`=1.0 WHERE NOT (`user_id`<=101);",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE NOT (`user_id`<=?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 101},
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
			sql:  "UPDATE order SET `content`='1',`account`=1.0 WHERE NOT (`user_id`=101);",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content`=?,`account`=? WHERE NOT (`user_id`=?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{"1", 1.0, 101},
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
			sql:  "UPDATE order SET `content`='1',`account`=1.0 WHERE NOT (`user_id`!=101);",
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content`=?,`account`=? WHERE NOT (`user_id`!=?);", "`order_db_1`", "`order_tab_2`"),
					Args:       []any{"1", 1.0, 101},
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
			handler, err := NewUpdateHandler(shardAlgorithm, dss, ctx)
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

type UpdateHandlerSuite struct {
	suite.Suite
	mock01   sqlmock.Sqlmock
	mockDB01 *sql.DB
	mock02   sqlmock.Sqlmock
	mockDB02 *sql.DB
}

func (s *UpdateHandlerSuite) SetupSuite() {
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

func (s *UpdateHandlerSuite) TearDownTest() {
	_ = s.mockDB01.Close()
	_ = s.mockDB02.Close()
}

func (s *UpdateHandlerSuite) TestUpdateHandler_Exec() {
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
	testCases := []struct {
		name             string
		sql              string
		mockDB           func()
		wantAffectedRows int64
		wantErr          error
	}{
		{
			name: "where eq",
			sql:  "UPDATE order  SET `order_id`=1,`content`='1',`account`=1.0 WHERE `user_id`=1;",
			mockDB: func() {
				s.mock02.ExpectExec(regexp.QuoteMeta("UPDATE `order_db_1`.`order_tab_1` SET `order_id`=?,`content`=?,`account`=? WHERE `user_id`=?;")).
					WithArgs(1, "1", 1.0, 1).WillReturnResult(sqlmock.NewResult(1, 1))
			},
			wantAffectedRows: 1,
		},
		{
			name: "where or",
			sql:  "UPDATE order  SET `content`='1',`account`=1.0 WHERE (`user_id`=123) OR (`user_id`=234);",
			mockDB: func() {
				s.mock02.ExpectExec(regexp.QuoteMeta("UPDATE `order_db_1`.`order_tab_0` SET `content`=?,`account`=? WHERE (`user_id`=?) OR (`user_id`=?);")).
					WithArgs("1", 1.0, 123, 234).WillReturnResult(sqlmock.NewResult(1, 2))
				s.mock01.ExpectExec(regexp.QuoteMeta("UPDATE `order_db_0`.`order_tab_0` SET `content`=?,`account`=? WHERE (`user_id`=?) OR (`user_id`=?);")).
					WithArgs("1", 1.0, 123, 234).WillReturnResult(sqlmock.NewResult(1, 2))
			},
			wantAffectedRows: 4,
		},
	}
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			tc.mockDB()
			ctx := &pcontext.Context{
				Context: context.Background(),
				Query:   tc.sql,
				ParsedQuery: pcontext.ParsedQuery{
					Root: ast.Parse(tc.sql),
				},
			}
			handler, err := NewUpdateHandler(shardAlgorithm, dss, ctx)
			require.NoError(t, err)
			res := handler.Exec(context.Background())
			require.Equal(t, tc.wantErr, res.Err())
			if res.Err() != nil {
				return
			}
			affectRows, err := res.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, tc.wantAffectedRows, affectRows)
		})
	}
}
func TestUpdateHandlerSuite(t *testing.T) {
	suite.Run(t, &UpdateHandlerSuite{})
}
