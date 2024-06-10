package sharding

import (
	"context"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/cluster"
	"github.com/meoying/dbproxy/internal/datasource/masterslave"
	"github.com/meoying/dbproxy/internal/datasource/masterslave/slaves"
	"github.com/meoying/dbproxy/internal/datasource/masterslave/slaves/roundrobin"
	"github.com/meoying/dbproxy/internal/datasource/shardingsource"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	pcontext "github.com/meoying/dbproxy/internal/protocol/mysql/plugin/context"
	"github.com/meoying/dbproxy/internal/sharding"
	"github.com/meoying/dbproxy/internal/sharding/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

type Order struct {
	UserId  int
	OrderId int64
	Content string
	Account float64
}

func TestShardingSelector_Build(t *testing.T) {
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
		qs      []sharding.Query
		wantErr error
	}{
		{
			name: "only eq",
			sql:  "SELECT `user_id`,`order_id`,`content`,`account` FROM order  WHERE `user_id`=123;",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_0` WHERE `user_id` = 123 ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "only eq broadcast",
			sql:  "SELECT `user_id`,`order_id`,`content`,`account` FROM order  WHERE `order_id`=123;",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_0` WHERE `order_id` = 123 ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_1` WHERE `order_id` = 123 ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_2` WHERE `order_id` = 123 ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_0` WHERE `order_id` = 123 ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_1` WHERE `order_id` = 123 ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_2` WHERE `order_id` = 123 ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and left",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE (`order_id`=12) AND (`user_id`=123);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ( `order_id` = 12 ) AND ( `user_id` = 123 ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id`=123) OR (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ( `user_id` = 123 ) OR ( `user_id` = 234 ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ( `user_id` = 123 ) OR ( `user_id` = 234 ) ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or left broadcast",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE (`order_id`=12) OR (`user_id`=123);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ( `order_id` = 12 ) OR ( `user_id` = 123 ) ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE ( `order_id` = 12 ) OR ( `user_id` = 123 ) ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE ( `order_id` = 12 ) OR ( `user_id` = 123 ) ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ( `order_id` = 12 ) OR ( `user_id` = 123 ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE ( `order_id` = 12 ) OR ( `user_id` = 123 ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE ( `order_id` = 12 ) OR ( `user_id` = 123 ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or right broadcast",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id`=123) OR (`order_id`=12);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ( `user_id` = 123 ) OR ( `order_id` = 12 ) ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE ( `user_id` = 123 ) OR ( `order_id` = 12 ) ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE ( `user_id` = 123 ) OR ( `order_id` = 12 ) ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ( `user_id` = 123 ) OR ( `order_id` = 12 ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE ( `user_id` = 123 ) OR ( `order_id` = 12 ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE (`user_id`=?) OR (`order_id`=?);",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or all",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ( ( `user_id` = 123 ) AND ( `order_id` = 12 ) ) OR ( `user_id` = 234 ) ; ",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ( ( `user_id` = 123 ) AND ( `order_id` = 12 ) ) OR ( `user_id` = 234 ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ( ( `user_id` = 123 ) AND ( `order_id` = 12 ) ) OR ( `user_id` = 234 ) ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ( `user_id` = 123 ) AND ( ( `order_id` = 12 ) OR ( `user_id` = 234 ) ) ; ",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ( `user_id` = 123 ) AND ( ( `order_id` = 12 ) OR ( `user_id` = 234 ) ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and all",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ( `user_id` = 123 ) OR ( ( `user_id` = 181 ) AND ( `user_id` = 234 ) ) ; ",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ( `user_id` = 123 ) OR ( ( `user_id` = 181 ) AND ( `user_id` = 234 ) ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ( ( `user_id` = 123 ) OR ( `user_id` = 234 ) ) AND ( `order_id` = 24 ) ; ",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ( ( `user_id` = 123 ) OR ( `user_id` = 234 ) ) AND ( `order_id` = 24 ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ( ( `user_id` = 123 ) OR ( `user_id` = 234 ) ) AND ( `order_id` = 24 ) ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and broadcast",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ( ( `user_id` = 123 ) OR ( `order_id` = 12 ) ) AND ( `user_id` = 234 ) ; ",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ( ( `user_id` = 123 ) OR ( `order_id` = 12 ) ) AND ( `user_id` = 234 ) ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ( ( `user_id` = 123 ) OR ( `user_id` = 253 ) ) OR ( `user_id` = 234 ) ; ",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ( ( `user_id` = 123 ) OR ( `user_id` = 253 ) ) OR ( `user_id` = 234 ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE ( ( `user_id` = 123 ) OR ( `user_id` = 253 ) ) OR ( `user_id` = 234 ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ( ( `user_id` = 123 ) OR ( `user_id` = 253 ) ) OR ( `user_id` = 234 ) ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or broadcast",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ( ( `user_id` = 123 ) OR ( `order_id` = 12 ) ) OR ( `user_id` = 234 ) ; ",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ( ( `user_id` = 123 ) OR ( `order_id` = 12 ) ) OR ( `user_id` = 234 ) ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE ( ( `user_id` = 123 ) OR ( `order_id` = 12 ) ) OR ( `user_id` = 234 ) ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE ( ( `user_id` = 123 ) OR ( `order_id` = 12 ) ) OR ( `user_id` = 234 ) ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ( ( `user_id` = 123 ) OR ( `order_id` = 12 ) ) OR ( `user_id` = 234 ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE ( ( `user_id` = 123 ) OR ( `order_id` = 12 ) ) OR ( `user_id` = 234 ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE ( ( `user_id` = 123 ) OR ( `order_id` = 12 ) ) OR ( `user_id` = 234 ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-and",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ( ( `user_id` = 123 ) AND ( `order_id` = 12 ) ) AND ( `order_id` = 23 ) ; ",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ( ( `user_id` = 123 ) AND ( `order_id` = 12 ) ) AND ( `order_id` = 23 ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or-and",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ( ( `user_id` = 123 ) AND ( `order_id` = 12 ) ) OR ( ( `user_id` = 234 ) AND ( `order_id` = 18 ) ) ; ",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ( ( `user_id` = 123 ) AND ( `order_id` = 12 ) ) OR ( ( `user_id` = 234 ) AND ( `order_id` = 18 ) ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ( ( `user_id` = 123 ) AND ( `order_id` = 12 ) ) OR ( ( `user_id` = 234 ) AND ( `order_id` = 18 ) ) ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where lt",
			sql:  "SELECT `order_id`,`content` FROM order WHERE `user_id`<1;",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id` < 1 ; "
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
			name: "where lt eq",
			sql:  "SELECT `order_id`,`content` FROM order WHERE `user_id`<=1;",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id` <= 1 ; "
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
			name: "where gt",
			sql:  "SELECT `order_id`,`content` FROM order WHERE `user_id`>1;",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id` > 1 ; "
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
			name: "where gt eq",
			sql:  "SELECT `order_id`,`content` FROM order WHERE `user_id`>=1;",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id` >= 1 ; "
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
			name: "where eq and lt or gt",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ( ( `user_id` = 12 ) AND ( `user_id` < 133 ) ) OR ( `user_id` > 234 ) ; ",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE ( ( `user_id` = 12 ) AND ( `user_id` < 133 ) ) OR ( `user_id` > 234 ) ; "
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
			name: "where in",
			sql:  "SELECT `order_id`,`content` FROM order WHERE `user_id` IN (12,35,101);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE `user_id` IN ( 12 , 35 , 101 ) ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE `user_id` IN ( 12 , 35 , 101 ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in and eq",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id` IN (12,35,101)) AND (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ( `user_id` IN ( 12 , 35 , 101 ) ) AND ( `user_id` = 234 ) ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in or eq",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id` IN (12,35,101)) OR (`user_id`=531);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ( `user_id` IN ( 12 , 35 , 101 ) ) OR ( `user_id` = 531 ) ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ( `user_id` IN ( 12 , 35 , 101 ) ) OR ( `user_id` = 531 ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE ( `user_id` IN ( 12 , 35 , 101 ) ) OR ( `user_id` = 531 ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in or gt",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id` IN (12,35,101)) OR (`user_id`>531);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE ( `user_id` IN ( 12 , 35 , 101 ) ) OR ( `user_id` > 531 ) ; "
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
			name: "where not in",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE `user_id` NOT IN (12,35,101);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id` NOT IN ( 12 , 35 , 101 ) ; "
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
			name: "where not in and eq",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ( `user_id` NOT IN ( 12 , 35 , 101 ) ) AND ( `user_id` = 234 ) ; ",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ( `user_id` NOT IN ( 12 , 35 , 101 ) ) AND ( `user_id` = 234 ) ; ",
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not in or eq",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE ( `user_id` NOT IN ( 12 , 35 , 101 ) ) OR ( `user_id` = 531 ) ; ",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE ( `user_id` NOT IN ( 12 , 35 , 101 ) ) OR ( `user_id` = 531 ) ; "
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
			name: "where not in or gt",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE ( `user_id` NOT IN ( 12 , 35 , 101 ) ) OR ( `user_id` > 531 ) ; ",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE ( `user_id` NOT IN ( 12 , 35 , 101 ) ) OR ( `user_id` > 531 ) ; "
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
			name: "where not gt",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ( `user_id` > 101 ) ; ",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ( `user_id` > 101 ) ; "
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
			name: "where not lt",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ( `user_id` < 101 ) ; ",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ( `user_id` < 101 ) ; "
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
			name: "where not gt eq",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ( `user_id` >= 101 ) ; ",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ( `user_id` >= 101 ) ; "
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
			name: "where not lt eq",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ( `user_id` <= 101 ) ; ",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ( `user_id` <= 101 ) ; "
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
			name: "where not eq",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ( `user_id` = 101 ) ; ",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ( `user_id` = 101 ) ; "
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
			name: "where not neq",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ( `user_id` != 101 ) ; ",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE NOT ( `user_id` != 101 ) ; ",
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not (gt and lt)",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ( ( `user_id` > 12 ) AND ( `user_id` < 531 ) ) ; ",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ( ( `user_id` > 12 ) AND ( `user_id` < 531 ) ) ; "
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
			name: "where not (gt eq and lt eq)",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ( ( `user_id` >= 12 ) AND ( `user_id` <= 531 ) ) ; ",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ( ( `user_id` >= 12 ) AND ( `user_id` <= 531 ) ) ; "
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
			name: "where not (in or gt)",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ( ( `user_id` IN ( 12 , 35 , 101 ) ) OR ( `user_id` > 531 ) ) ; ",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ( ( `user_id` IN ( 12 , 35 , 101 ) ) OR ( `user_id` > 531 ) ) ; "
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
			name: "where not (in or eq)",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ( ( `user_id` IN ( 12 , 35 , 101 ) ) OR ( `user_id` = 531 ) ) ; ",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ( ( `user_id` IN ( 12 , 35 , 101 ) ) OR ( `user_id` = 531 ) ) ; "
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
			name: "where not (eq and eq)",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ( ( `user_id` = 12 ) AND ( `user_id` = 531 ) ) ; ",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ( ( `user_id` = 12 ) AND ( `user_id` = 531 ) ) ; "
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
			name: "select from",
			sql:  "SELECT `order_id`,`content` FROM order;",
			qs: func() []sharding.Query {
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
		//{
		//	name: "select 列中带有聚合函数COUNT(*)",
		//	sql:  "select count(*) from order ",
		//	qs: func() []sharding.Query {
		//		var res []sharding.Query
		//		sql := "SELECT COUNT(*) FROM `%s`.`%s`;"
		//		for i := 0; i < dbBase; i++ {
		//			dbName := fmt.Sprintf(dbPattern, i)
		//			for j := 0; j < tableBase; j++ {
		//				tableName := fmt.Sprintf(tablePattern, j)
		//				res = append(res, sharding.Query{
		//					SQL:        fmt.Sprintf(sql, dbName, tableName),
		//					DB:         dbName,
		//					Datasource: dsPattern,
		//				})
		//			}
		//		}
		//		return res
		//	}(),
		//},
		//{
		//	name: "select 列中带有聚合函数COUNT(1)",
		//	sql:  "select count(1) from order ",
		//	qs: func() []sharding.Query {
		//		var res []sharding.Query
		//		sql := "SELECT COUNT(1) FROM `%s`.`%s`;"
		//		for i := 0; i < dbBase; i++ {
		//			dbName := fmt.Sprintf(dbPattern, i)
		//			for j := 0; j < tableBase; j++ {
		//				tableName := fmt.Sprintf(tablePattern, j)
		//				res = append(res, sharding.Query{
		//					SQL:        fmt.Sprintf(sql, dbName, tableName),
		//					DB:         dbName,
		//					Datasource: dsPattern,
		//				})
		//			}
		//		}
		//		return res
		//	}(),
		//},
		//{
		//	name: "聚合函数AVG(`id`)",
		//	sql:  "select AVG(`id`) from order ",
		//	qs: func() []sharding.Query {
		//		var res []sharding.Query
		//		sql := "SELECT SUM(`id`),COUNT(`id`) FROM `%s`.`%s`;"
		//		for i := 0; i < dbBase; i++ {
		//			dbName := fmt.Sprintf(dbPattern, i)
		//			for j := 0; j < tableBase; j++ {
		//				tableName := fmt.Sprintf(tablePattern, j)
		//				res = append(res, sharding.Query{
		//					SQL:        fmt.Sprintf(sql, dbName, tableName),
		//					DB:         dbName,
		//					Datasource: dsPattern,
		//				})
		//			}
		//		}
		//		return res
		//	}(),
		//},
		//{
		//	name: "Distinct单个字段",
		//	sql:  "select distinct id from order;",
		//	qs: func() []sharding.Query {
		//		var res []sharding.Query
		//		sql := "SELECT DISTINCT `id` FROM `%s`.`%s`;"
		//		for i := 0; i < dbBase; i++ {
		//			dbName := fmt.Sprintf(dbPattern, i)
		//			for j := 0; j < tableBase; j++ {
		//				tableName := fmt.Sprintf(tablePattern, j)
		//				res = append(res, sharding.Query{
		//					SQL:        fmt.Sprintf(sql, dbName, tableName),
		//					DB:         dbName,
		//					Datasource: dsPattern,
		//				})
		//			}
		//		}
		//		return res
		//	}(),
		//},
		//{
		//	name: "聚合函数和distinct",
		//	sql:  "select Avg(distinct id) from order;",
		//	qs: func() []sharding.Query {
		//		var res []sharding.Query
		//		sql := "SELECT SUM(DISTINCT id),COUNT(DISTINCT id) FROM `%s`.`%s`;"
		//		for i := 0; i < dbBase; i++ {
		//			dbName := fmt.Sprintf(dbPattern, i)
		//			for j := 0; j < tableBase; j++ {
		//				tableName := fmt.Sprintf(tablePattern, j)
		//				res = append(res, sharding.Query{
		//					SQL:        fmt.Sprintf(sql, dbName, tableName),
		//					DB:         dbName,
		//					Datasource: dsPattern,
		//				})
		//			}
		//		}
		//		return res
		//	}(),
		//},
		//{
		//	name: "group by",
		//	sql:  "select `order_id`,count(`user_id`) from order group by `order_id`;",
		//	qs: func() []sharding.Query {
		//		var res []sharding.Query
		//		sql := "SELECT `order_id`,COUNT(`user_id`) FROM `%s`.`%s` GROUP BY `order_id`;"
		//		for i := 0; i < dbBase; i++ {
		//			dbName := fmt.Sprintf(dbPattern, i)
		//			for j := 0; j < tableBase; j++ {
		//				tableName := fmt.Sprintf(tablePattern, j)
		//				res = append(res, sharding.Query{
		//					SQL:        fmt.Sprintf(sql, dbName, tableName),
		//					DB:         dbName,
		//					Datasource: dsPattern,
		//				})
		//			}
		//		}
		//		return res
		//	}(),
		//},
		//{
		//	name: "order by",
		//	sql:  "select * from order order by  `order_id`,`user_id` desc;",
		//	qs: func() []sharding.Query {
		//		var res []sharding.Query
		//		sql := "SELECT * FROM `%s`.`%s` ORDER BY `order_id` ASC,`user_id` DESC;"
		//		for i := 0; i < dbBase; i++ {
		//			dbName := fmt.Sprintf(dbPattern, i)
		//			for j := 0; j < tableBase; j++ {
		//				tableName := fmt.Sprintf(tablePattern, j)
		//				res = append(res, sharding.Query{
		//					SQL:        fmt.Sprintf(sql, dbName, tableName),
		//					DB:         dbName,
		//					Datasource: dsPattern,
		//				})
		//			}
		//		}
		//		return res
		//	}(),
		//},
		//{
		//	name: "limit",
		//	sql:  "select * from order order by  `user_id` desc limit 10 offset 5 ;",
		//	qs: func() []sharding.Query {
		//		var res []sharding.Query
		//		sql := "SELECT * FROM `%s`.`%s` ORDER BY `user_id` DESC LIMIT ?;"
		//		for i := 0; i < dbBase; i++ {
		//			dbName := fmt.Sprintf(dbPattern, i)
		//			for j := 0; j < tableBase; j++ {
		//				tableName := fmt.Sprintf(tablePattern, j)
		//				res = append(res, sharding.Query{
		//					SQL:        fmt.Sprintf(sql, dbName, tableName),
		//					DB:         dbName,
		//					Args:       []any{15},
		//					Datasource: dsPattern,
		//				})
		//			}
		//		}
		//		return res
		//	}(),
		//},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			ctx := &pcontext.Context{
				Context: context.Background(),
				Query:   tc.sql,
				ParsedQuery: pcontext.ParsedQuery{
					Root: ast.Parse(tc.sql),
				},
			}
			handler, err := NewSelectHandler(shardAlgorithm, dss, ctx)
			require.NoError(t, err)
			res, err := handler.Build(context.Background())
			assert.Equal(t, c.wantErr, err)
			if err != nil {
				return
			}
			assert.ElementsMatch(t, c.qs, res)
		})
	}
}

func TestShardingSelector_GetMulti(t *testing.T) {
	shardAlgorithm := &hash.Hash{
		ShardingKey:  "order_id",
		DBPattern:    &hash.Pattern{Name: "order_detail_db_%d", Base: 2},
		TablePattern: &hash.Pattern{Name: "order_detail_tab_%d", Base: 3},
		DsPattern:    &hash.Pattern{Name: "0.db.cluster.company.com:3306", NotSharding: true},
	}
	mockDB, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mockDB.Close() }()

	rbSlaves, err := roundrobin.NewSlaves(mockDB)
	require.NoError(t, err)
	masterSlaveDB := masterslave.NewMasterSlavesDB(
		mockDB, masterslave.MasterSlavesWithSlaves(newMockSlaveNameGet(rbSlaves)))
	require.NoError(t, err)

	mockDB2, mock2, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mockDB2.Close() }()

	rbSlaves2, err := roundrobin.NewSlaves(mockDB2)
	require.NoError(t, err)
	masterSlaveDB2 := masterslave.NewMasterSlavesDB(
		mockDB2, masterslave.MasterSlavesWithSlaves(newMockSlaveNameGet(rbSlaves2)))
	require.NoError(t, err)

	clusterDB := cluster.NewClusterDB(map[string]*masterslave.MasterSlavesDB{
		"order_detail_db_0": masterSlaveDB,
		"order_detail_db_1": masterSlaveDB2,
	})
	ds := map[string]datasource.DataSource{
		"0.db.cluster.company.com:3306": clusterDB,
	}
	dss := shardingsource.NewShardingDataSource(ds)
	testCases := []struct {
		name      string
		sql       string
		mockOrder func(mock1, mock2 sqlmock.Sqlmock)
		wantErr   error
		wantRes   []*OrderDetail
	}{
		{
			name: "found tab or",
			sql:  "SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM order_detail WHERE (`order_id`=123) or (`order_id` = 234);",
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {
				rows1 := mock1.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"})
				rows1.AddRow(234, 12, "Kevin", "Durant")
				mock1.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE ( `order_id` = 123 ) or ( `order_id` = 234 ) ; ").WillReturnRows(rows1)
				rows2 := mock2.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"})
				rows2.AddRow(123, 10, "LeBron", "James")
				mock2.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE ( `order_id` = 123 ) or ( `order_id` = 234 ) ; ").WillReturnRows(rows2)
			},
			wantRes: []*OrderDetail{
				{OrderId: 123, ItemId: 10, UsingCol1: "LeBron", UsingCol2: "James"},
				{OrderId: 234, ItemId: 12, UsingCol1: "Kevin", UsingCol2: "Durant"},
			},
		},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			tc.mockOrder(mock, mock2)
			ctx := &pcontext.Context{
				Context: context.Background(),
				Query:   tc.sql,
				ParsedQuery: pcontext.ParsedQuery{
					Root: ast.Parse(tc.sql),
				},
			}
			handler, err := NewSelectHandler(shardAlgorithm, dss, ctx)
			require.NoError(t, err)
			queryRes, err := handler.QueryOrExec(context.Background())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			res := make([]*OrderDetail, 0, 16)
			if queryRes.Rows != nil {
				row := queryRes.Rows
				for row.Next() {
					d := &OrderDetail{}
					err = row.Scan(&d.OrderId, &d.ItemId, &d.UsingCol1, &d.UsingCol2)
					require.NoError(t, err)
					res = append(res, d)
				}
			}

			assert.ElementsMatch(t, c.wantRes, res)
		})
	}
}

type OrderDetail struct {
	OrderId   int
	ItemId    int
	UsingCol1 string
	UsingCol2 string
}

type testSlaves struct {
	slaves.Slaves
}

func newMockSlaveNameGet(s slaves.Slaves) *testSlaves {
	return &testSlaves{
		Slaves: s,
	}
}

func (s *testSlaves) Next(ctx context.Context) (slaves.Slave, error) {
	slave, err := s.Slaves.Next(ctx)
	if err != nil {
		return slave, err
	}
	return slave, err
}
