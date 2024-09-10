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
			sql:  "UPDATE  order SET `order_id`=1,`content`='1',`account`=1.0 WHERE `user_id` = 1;",
			wantQs: []sharding.Query{
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `order_id` = 1 , `content` = '1' , `account` = 1.0 WHERE `user_id` = 1 ; ", "`order_db_1`", "`order_tab_1`"),
					Table:      "order_tab_1",
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
				sql := "UPDATE `%s`.`%s` SET `content` = '1' , `account` = 1.0 ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Table:      tableName,
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
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content` = '1' , `account` = 1.0 WHERE ( `user_id` = 123 ) OR ( `user_id` = 234 ) ; ", "`order_db_1`", "`order_tab_0`"),
					Table:      "order_tab_0",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content` = '1' , `account` = 1.0 WHERE ( `user_id` = 123 ) OR ( `user_id` = 234 ) ; ", "`order_db_0`", "`order_tab_0`"),
					Table:      "order_tab_0",
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
				sql := "UPDATE `%s`.`%s` SET `content` = '1' , `account` = 1.0 WHERE ( `user_id` = 123 ) OR ( `order_id` = 2 ) ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Table:      tableName,
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
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content` = '1' , `account` = 1.0 WHERE ( ( `user_id` = 123 ) AND ( `order_id` = 12 ) ) OR ( `user_id` = 234 ) ; ", "`order_db_1`", "`order_tab_0`"),
					Table:      "order_tab_0",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content` = '1' , `account` = 1.0 WHERE ( ( `user_id` = 123 ) AND ( `order_id` = 12 ) ) OR ( `user_id` = 234 ) ; ", "`order_db_0`", "`order_tab_0`"),
					Table:      "order_tab_0",
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
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content` = '1' , `account` = 1.0 WHERE ( `user_id` = 123 ) OR ( ( `user_id` = 181 ) AND ( `user_id` = 234 ) ) ; ", "`order_db_1`", "`order_tab_0`"),
					Table:      "order_tab_0",
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
				sql := "UPDATE `%s`.`%s` SET `content` = '1' , `account` = 1.0 WHERE `user_id` < 123 ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Table:      tableName,
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
				sql := "UPDATE `%s`.`%s` SET `content` = '1' , `account` = 1.0 WHERE `user_id` <= 123 ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Table:      tableName,
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
				sql := "UPDATE `%s`.`%s` SET `content` = '1' , `account` = 1.0 WHERE `user_id` > 123 ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Table:      tableName,
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
				sql := "UPDATE `%s`.`%s` SET `content` = '1' , `account` = 1.0 WHERE `user_id` >= 123 ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Table:      tableName,
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
				sql := "UPDATE `%s`.`%s` SET `content` = '1' , `account` = 1.0 WHERE ( ( `user_id` = 12 ) AND ( `user_id` < 133 ) ) OR ( `user_id` > 234 ) ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Table:      tableName,
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
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content` = '1' , `account` = 1.0 WHERE `user_id` IN ( 12 , 35 , 101 ) ; ", "`order_db_1`", "`order_tab_2`"),
					Table:      "order_tab_2",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content` = '1' , `account` = 1.0 WHERE `user_id` IN ( 12 , 35 , 101 ) ; ", "`order_db_0`", "`order_tab_0`"),
					Table:      "order_tab_0",
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
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content` = '1' , `account` = 1.0 WHERE ( `user_id` IN ( 12 , 35 , 101 ) ) AND ( `user_id` = 234 ) ; ", "`order_db_0`", "`order_tab_0`"),
					Table:      "order_tab_0",
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
					SQL:        "UPDATE `order_db_1`.`order_tab_2` SET `content` = '1' , `account` = 1.0 WHERE ( `user_id` IN ( 12 , 35 , 101 ) ) OR ( `user_id` = 531 ) ; ",
					Table:      "order_tab_2",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "UPDATE `order_db_1`.`order_tab_0` SET `content` = '1' , `account` = 1.0 WHERE ( `user_id` IN ( 12 , 35 , 101 ) ) OR ( `user_id` = 531 ) ; ",
					Table:      "order_tab_0",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "UPDATE `order_db_0`.`order_tab_0` SET `content` = '1' , `account` = 1.0 WHERE ( `user_id` IN ( 12 , 35 , 101 ) ) OR ( `user_id` = 531 ) ; ",
					Table:      "order_tab_0",
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
				sql := "UPDATE `%s`.`%s` SET `content` = '1' , `account` = 1.0 WHERE `user_id` NOT IN ( 12 , 35 , 101 ) ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Table:      tableName,
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
				sql := "UPDATE `%s`.`%s` SET `content` = '1' , `account` = 1.0 WHERE NOT ( `user_id` > 101 ) ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Table:      tableName,
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
				sql := "UPDATE `%s`.`%s` SET `content` = '1' , `account` = 1.0 WHERE NOT ( `user_id` < 101 ) ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Table:      tableName,
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
			sql:  "UPDATE order SET `content`= '1',`account`=1.0 WHERE NOT (`user_id`>=101);",
			wantQs: func() []sharding.Query {
				var res []sharding.Query
				sql := "UPDATE `%s`.`%s` SET `content` = '1' , `account` = 1.0 WHERE NOT ( `user_id` >= 101 ) ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Table:      tableName,
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
				sql := "UPDATE `%s`.`%s` SET `content` = '1' , `account` = 1.0 WHERE NOT ( `user_id` <= 101 ) ; "
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(orderDBPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(orderTablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Table:      tableName,
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
					SQL:        fmt.Sprintf("UPDATE %s.%s SET `content` = '1' , `account` = 1.0 WHERE NOT ( `user_id` != 101 ) ; ", "`order_db_1`", "`order_tab_2`"),
					Table:      "order_tab_2",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
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
				s.mock02.ExpectExec(regexp.QuoteMeta("UPDATE `order_db_1`.`order_tab_1` SET `order_id` = 1 , `content` = '1' , `account` = 1.0 WHERE `user_id` = 1 ; ")).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
			wantAffectedRows: 1,
		},
		{
			name: "where or",
			sql:  "UPDATE order  SET `content`='1',`account`=1.0 WHERE (`user_id`=123) OR (`user_id`=234);",
			mockDB: func() {
				s.mock02.ExpectExec(regexp.QuoteMeta("UPDATE `order_db_1`.`order_tab_0` SET `content` = '1' , `account` = 1.0 WHERE ( `user_id` = 123 ) OR ( `user_id` = 234 ) ; ")).WillReturnResult(sqlmock.NewResult(1, 2))
				s.mock01.ExpectExec(regexp.QuoteMeta("UPDATE `order_db_0`.`order_tab_0` SET `content` = '1' , `account` = 1.0 WHERE ( `user_id` = 123 ) OR ( `user_id` = 234 ) ; ")).WillReturnResult(sqlmock.NewResult(1, 2))
			},
			wantAffectedRows: 4,
		},
	}
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			tc.mockDB()
			ctx := &pcontext.Context{
				Context:     context.Background(),
				Query:       tc.sql,
				ParsedQuery: pcontext.NewParsedQuery(tc.sql, vparser.NewHintVisitor()),
			}
			handler, err := NewUpdateHandler(shardAlgorithm, dss, ctx)
			require.NoError(t, err)
			res, err := handler.QueryOrExec(context.Background())
			require.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			affectRows, err := res.Result.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, tc.wantAffectedRows, affectRows)
		})
	}
}

func TestUpdateHandlerSuite(t *testing.T) {
	suite.Run(t, &UpdateHandlerSuite{})
}
