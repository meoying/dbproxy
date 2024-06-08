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

func TestShardingSelector_onlyDataSource_Build(t *testing.T) {
	dsBase := 2
	dbPattern, tablePattern, dsPattern := "order_db", "order_tab", "%d.db.cluster.company.com:3306"
	shardAlgorithm := &hash.Hash{
		ShardingKey:  "user_id",
		DBPattern:    &hash.Pattern{Name: dbPattern, NotSharding: true},
		TablePattern: &hash.Pattern{Name: tablePattern, NotSharding: true},
		DsPattern:    &hash.Pattern{Name: dsPattern, Base: dsBase},
	}

	m := map[string]*masterslave.MasterSlavesDB{
		"order_db": MasterSlavesMemoryDB(),
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
		qs      []sharding.Query
		wantErr error
	}{
		{
			name: "only eq",
			sql:  "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_tab` WHERE user_id = 123;",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db`.`order_tab` WHERE `user_id`=?;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "only eq broadcast",
			sql:  "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_tab` WHERE order_id = 123;",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db`.`order_tab` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db`.`order_tab` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "all columns",
			sql:  "select * from `order_tab` where `user_id`=123;",
			qs: []sharding.Query{
				{
					SQL:        "SELECT * FROM `order_db`.`order_tab` WHERE `user_id`=?;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and left",
			sql:  "select `order_id`,`content` from `order_tab` where (order_id=12) and (user_id = 123);  ",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`order_id`=?) AND (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and right",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=123) AND (`order_id`=12);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=?) AND (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=123) OR (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args:       []any{123, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args:       []any{123, 234},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or left broadcast",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`order_id`=12) OR (`user_id`=123);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or right broadcast",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=123) OR (`order_id`=12);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=123) AND (`order_id`=12)) OR (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or broadcast",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=123) AND ((`order_id`=12) OR (`user_id`=234));",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=?) AND ((`order_id`=?) OR (`user_id`=?));",
					Args:       []any{123, 12, 234},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and all",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=123) OR ((`user_id`=181) AND (`user_id`=234));",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));",
					Args:       []any{123, 181, 234},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=123) OR (`user_id`=234)) AND (`order_id`=24);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 234, 24},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 234, 24},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and broadcast",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=123) OR (`order_id`=12)) AND (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) OR (`order_id`=?)) AND (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=123) OR (`user_id`=253)) OR (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or broadcast",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=123) OR (`order_id`=12)) OR (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-and",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=123) AND (`order_id`=12)) AND (`order_id`=23);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) AND (`order_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 12, 23},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or-and",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=123) AND (`order_id`=12)) OR ((`user_id`=234) AND (`order_id`=18));",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args:       []any{123, 12, 234, 18},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args:       []any{123, 12, 234, 18},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where lt",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<1;",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<?;"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{1},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where lt eq",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<=1;",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<=?;"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{1},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where eq and lt",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=12) AND (`user_id`<133);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=?) AND (`user_id`<?);",
					Args:       []any{12, 133},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where gt",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>1;",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>?;"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{1},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where gt eq",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>=1;",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>=?;"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{1},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where eq and gt",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=12) AND (`user_id`>133);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id`=?) AND (`user_id`>?);",
					Args:       []any{12, 133},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where eq and lt or gt",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE ((`user_id`=12) AND (`user_id`<133)) OR (`user_id`>234);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE ((`user_id`=?) AND (`user_id`<?)) OR (`user_id`>?);"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{12, 133, 234},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where in",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE `user_id` IN (12,35,101);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE `user_id` IN (?,?,?);",
					Args:       []any{12, 35, 101},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE `user_id` IN (?,?,?);",
					Args:       []any{12, 35, 101},
					DB:         "order_db",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in and eq",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id` IN (12,35,101)) AND (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id` IN (?,?,?)) AND (`user_id`=?);",
					Args:       []any{12, 35, 101, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not in",
			sql:  "SELECT `order_id`,`content` FROM `order_tab`  WHERE `user_id` NOT IN (12,35,101);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id` NOT IN (?,?,?);"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{12, 35, 101},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where not in and eq",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id` NOT IN (12,35,101)) AND (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab` WHERE (`user_id` NOT IN (?,?,?)) AND (`user_id`=?);",
					Args:       []any{12, 35, 101, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not in or eq",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (12,35,101)) OR (`user_id`=531);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (?,?,?)) OR (`user_id`=?);"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{12, 35, 101, 531},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where not in or gt",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (12,35,101)) OR (`user_id`>531);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (?,?,?)) OR (`user_id`>?);"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{12, 35, 101, 531},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where not gt",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`>101);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`>?);"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{101},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where not lt",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`<101);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`<?);"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{101},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (gt and lt)",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>12) AND (`user_id`<531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>?) AND (`user_id`<?));"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{12, 531},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (gt eq and lt eq)",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>=12) AND (`user_id`<=531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>=?) AND (`user_id`<=?));"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{12, 531},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (in or gt)",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (12,35,101)) OR (`user_id`>531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (?,?,?)) OR (`user_id`>?));"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{12, 35, 101, 531},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (in or eq)",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (12,35,101)) OR (`user_id`=531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (?,?,?)) OR (`user_id`=?));"
				for b := 0; b < dsBase; b++ {
					dsName := fmt.Sprintf(dsPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tablePattern),
						Args:       []any{12, 35, 101, 531},
						DB:         dbPattern,
						Datasource: dsName,
					})
				}
				return res
			}(),
		},
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

func TestShardingSelector_onlyTable_Build(t *testing.T) {
	tableBase := 3
	dbPattern, tablePattern, dsPattern := "order_db", "order_tab_%d", "0.db.cluster.company.com:3306"
	shardAlgorithm := &hash.Hash{
		ShardingKey:  "user_id",
		DBPattern:    &hash.Pattern{Name: dbPattern, NotSharding: true},
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
			sql:  "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db`.`order_tab_0` WHERE `user_id`=123;",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db`.`order_tab_0` WHERE `user_id`=?;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "only eq broadcast",
			sql:  "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db`.`order_tab_0` WHERE `order_id`=123;",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db`.`order_tab_0` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db`.`order_tab_1` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db`.`order_tab_2` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and left",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`order_id`=12) AND (`user_id`=123);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`order_id`=?) AND (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and right",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=123) AND (`order_id`=12);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=?) AND (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=123) OR (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args:       []any{123, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or left broadcast",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`order_id`=12) OR (`user_id`=123);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_1` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_2` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or right broadcast",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=123) OR (`order_id`=12);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_1` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_2` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE ((`user_id`=123) AND (`order_id`=12)) OR (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or broadcast",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=123) AND ((`order_id`=12) OR (`user_id`=234));",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=?) AND ((`order_id`=?) OR (`user_id`=?));",
					Args:       []any{123, 12, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and all",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=123) OR ((`user_id`=181) AND (`user_id`=234));",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));",
					Args:       []any{123, 181, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and all",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=123) OR ((`user_id`=181) AND (`user_id`=234));",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));",
					Args:       []any{123, 181, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE ((`user_id`=123) OR (`user_id`=234)) AND (`order_id`=24);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 234, 24},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and broadcast",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE ((`user_id`=123) OR (`order_id`=12)) AND (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) AND (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE ((`user_id`=123) OR (`user_id`=253)) OR (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_1` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or broadcast",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE ((`user_id`=123) OR (`order_id`=12)) OR (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_1` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_2` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-and",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE ((`user_id`=123) AND (`order_id`=12)) AND (`order_id`=23);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 12, 23},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or-and",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ((`user_id`=123) AND (`order_id`=12)) OR ((`user_id`=234) AND (`order_id`=18));",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args:       []any{123, 12, 234, 18},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where lt",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE `user_id`<1;",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<?;"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{1},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where lt eq",
			sql:  "SELECT `order_id`,`content` FROM order WHERE `user_id`<=1;",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<=?;"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{1},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where eq and lt",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE (`user_id`=12) AND (`user_id`<133);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=?) AND (`user_id`<?);",
					Args:       []any{12, 133},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where gt",
			sql:  "SELECT `order_id`,`content` FROM order WHERE `user_id`>1;",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>?;"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{1},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where gt eq",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE `user_id`>=1;",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>=?;"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{1},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where eq and gt",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id`=12) AND (`user_id`>133);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id`=?) AND (`user_id`>?);",
					Args:       []any{12, 133},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where eq and lt or gt",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE ((`user_id`=12) AND (`user_id`<133)) OR (`user_id`>234);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE ((`user_id`=?) AND (`user_id`<?)) OR (`user_id`>?);"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{12, 133, 234},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where in",
			sql:  "SELECT `order_id`,`content` FROM order WHERE `user_id` IN (12,35,101);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_2` WHERE `user_id` IN (?,?,?);",
					Args:       []any{12, 35, 101},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE `user_id` IN (?,?,?);",
					Args:       []any{12, 35, 101},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in and eq",
			sql:  "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id` IN (12,35,101)) AND (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id` IN (?,?,?)) AND (`user_id`=?);",
					Args:       []any{12, 35, 101, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not in",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE `user_id` NOT IN (12,35,101);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id` NOT IN (?,?,?);"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{12, 35, 101},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not in and eq",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE (`user_id` NOT IN (12,35,101)) AND (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db`.`order_tab_0` WHERE (`user_id` NOT IN (?,?,?)) AND (`user_id`=?);",
					Args:       []any{12, 35, 101, 234},
					DB:         "order_db",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not in or eq",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id` NOT IN (12,35,101)) OR (`user_id`=531);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (?,?,?)) OR (`user_id`=?);"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{12, 35, 101, 531},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not in or gt",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE (`user_id` NOT IN (12,35,101)) OR (`user_id`>531);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (?,?,?)) OR (`user_id`>?);"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{12, 35, 101, 531},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not gt",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT (`user_id`>101);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`>?);"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{101},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not lt",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT (`user_id`<101);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`<?);"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{101},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (gt and lt)",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ((`user_id`>12) AND (`user_id`<531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>?) AND (`user_id`<?));"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{12, 531},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (gt eq and lt eq)",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ((`user_id`>=12) AND (`user_id`<=531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>=?) AND (`user_id`<=?));"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{12, 531},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (in or gt)",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ((`user_id` IN (12,35,101)) OR (`user_id`>531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (?,?,?)) OR (`user_id`>?));"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{12, 35, 101, 531},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (in or eq)",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ((`user_id` IN (12,35,101)) OR (`user_id`=531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (?,?,?)) OR (`user_id`=?));"
				for b := 0; b < tableBase; b++ {
					tableName := fmt.Sprintf(tablePattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbPattern, tableName),
						Args:       []any{12, 35, 101, 531},
						DB:         dbPattern,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
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

func TestShardingSelector_onlyDB_Build(t *testing.T) {

	dbBase := 2
	dbPattern, tablePattern, dsPattern := "order_db_%d", "order_tab", "0.db.cluster.company.com:3306"
	shardAlgorithm := &hash.Hash{
		ShardingKey:  "user_id",
		DBPattern:    &hash.Pattern{Name: dbPattern, Base: dbBase},
		TablePattern: &hash.Pattern{Name: tablePattern, NotSharding: true},
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
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab` WHERE `user_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "only eq broadcast",
			sql:  "SELECT `user_id`,`order_id`,`content`,`account` FROM order WHERE `order_id`=123;",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "columns",
			sql:  "SELECT `content`,`order_id` FROM order WHERE `user_id`=123;",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `content`,`order_id` FROM `order_db_1`.`order_tab` WHERE `user_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and left",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`order_id`=12) AND (`user_id`=123);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE (`order_id`=?) AND (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and right",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id`=123) AND (`order_id`=12);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE (`user_id`=?) AND (`order_id`=?);",
					Args:       []any{123, 12},
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
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args:       []any{123, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args:       []any{123, 234},
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
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
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
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ((`user_id`=123) AND (`order_id`=12)) OR (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or broadcast",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id`=123) AND ((`order_id`=12) OR (`user_id`=234));",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE (`user_id`=?) AND ((`order_id`=?) OR (`user_id`=?));",
					Args:       []any{123, 12, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and all",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id`=123) OR ((`user_id`=181) AND (`user_id`=234));",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));",
					Args:       []any{123, 181, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ((`user_id`=123) OR (`user_id`=234)) AND (`order_id`=24);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 234, 24},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 234, 24},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and broadcast",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ((`user_id`=123) OR (`order_id`=12)) AND (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE ((`user_id`=?) OR (`order_id`=?)) AND (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or",
			sql:  "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE ((`user_id`=123) OR (`user_id`=253)) OR (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or broadcast",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ((`user_id`=123) OR (`order_id`=12)) OR (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-and",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ((`user_id`=123) AND (`order_id`=12)) AND (`order_id`=23);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE ((`user_id`=?) AND (`order_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 12, 23},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or-and",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ((`user_id`=123) AND (`order_id`=12)) OR ((`user_id`=234) AND (`order_id`=18));",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args:       []any{123, 12, 234, 18},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args:       []any{123, 12, 234, 18},
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
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<?;"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{1},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where lt eq",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE `user_id`<=1;",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<=?;"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{1},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where eq and lt",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id`=12) AND (`user_id`<133);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE (`user_id`=?) AND (`user_id`<?);",
					Args:       []any{12, 133},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where gt",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE `user_id`>1;",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>?;"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{1},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where gt eq",
			sql:  "SELECT `order_id`,`content` FROM order WHERE `user_id`>=1;",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>=?;"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{1},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where eq and gt",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id`=12) AND (`user_id`>133);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE (`user_id`=?) AND (`user_id`>?);",
					Args:       []any{12, 133},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where eq and lt or gt",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE ((`user_id`=12) AND (`user_id`<133)) OR (`user_id`>234);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE ((`user_id`=?) AND (`user_id`<?)) OR (`user_id`>?);"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{12, 133, 234},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where in",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE `user_id` IN (12,35,101);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE `user_id` IN (?,?,?);",
					Args:       []any{12, 35, 101},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE `user_id` IN (?,?,?);",
					Args:       []any{12, 35, 101},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in and eq",
			sql:  "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE (`user_id` IN (12,35,101)) AND (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE (`user_id` IN (?,?,?)) AND (`user_id`=?);",
					Args:       []any{12, 35, 101, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in or eq",
			sql:  "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE (`user_id` IN (12,35,101)) OR (`user_id`=531);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{12, 35, 101, 531},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab` WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{12, 35, 101, 531},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in or gt",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE (`user_id` IN (12,35,101)) OR (`user_id`=531);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{12, 35, 101, 531},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not in",
			sql:  "SELECT `order_id`,`content` FROM order WHERE `user_id` NOT IN (12,35,101);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id` NOT IN (?,?,?);"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{12, 35, 101},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not in and eq",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE (`user_id` NOT IN (12,35,101)) AND (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab` WHERE (`user_id` NOT IN (?,?,?)) AND (`user_id`=?);",
					Args:       []any{12, 35, 101, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not in or eq",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id` NOT IN (12,35,101)) OR (`user_id`=531);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (?,?,?)) OR (`user_id`=?);"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{12, 35, 101, 531},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not in or gt",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE (`user_id` NOT IN (12,35,101)) OR (`user_id`>531);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (?,?,?)) OR (`user_id`>?);"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{12, 35, 101, 531},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not gt",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT (`user_id`>101);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`>?);"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{101},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not lt",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT (`user_id`<101);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`<?);"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{101},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (gt and lt)",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ((`user_id`>12) AND (`user_id`<531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>?) AND (`user_id`<?));"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{12, 531},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (gt eq and lt eq)",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE NOT ((`user_id`>=12) AND (`user_id`<=531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>=?) AND (`user_id`<=?));"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{12, 531},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (in or gt)",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ((`user_id` IN (12,35,101)) OR (`user_id`>531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (?,?,?)) OR (`user_id`>?));"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{12, 35, 101, 531},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
		{
			name: "where not (in or eq)",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (12,35,101)) OR (`user_id`=531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (?,?,?)) OR (`user_id`=?));"
				for b := 0; b < dbBase; b++ {
					dbName := fmt.Sprintf(dbPattern, b)
					res = append(res, sharding.Query{
						SQL:        fmt.Sprintf(sql, dbName, tablePattern),
						Args:       []any{12, 35, 101, 531},
						DB:         dbName,
						Datasource: dsPattern,
					})
				}
				return res
			}(),
		},
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

func TestShardingSelector_all_Build(t *testing.T) {
	dbBase, tableBase, dsBase := 2, 3, 2
	dbPattern, tablePattern, dsPattern := "order_db_%d", "order_tab_%d", "%d.db.cluster.company.com:3306"
	shardAlgorithm := &hash.Hash{
		ShardingKey:  "user_id",
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
		qs      []sharding.Query
		wantErr error
	}{
		{
			name: "only eq",
			sql:  "SELECT `user_id`,`order_id`,`content`,`account` FROM order WHERE `user_id`=123;",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_0` WHERE `user_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "only eq broadcast",
			sql:  "SELECT `user_id`,`order_id`,`content`,`account` FROM order  WHERE `order_id`=123;",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_0` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_1` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_2` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_0` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_1` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_2` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_0` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_1` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_2` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_0` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_1` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_2` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and left",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`order_id`=12) AND (`user_id`=123);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`order_id`=?) AND (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and right",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id`=123) AND (`order_id`=12);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) AND (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id`=123) OR (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args:       []any{123, 234},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args:       []any{123, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or left broadcast",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`order_id`=12) OR (`user_id`=123);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or right broadcast",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id`=123) OR (`order_id`=12);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ((`user_id`=123) AND (`order_id`=12)) OR (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or broadcast",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id`=123) AND ((`order_id`=12) OR (`user_id`=234));",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) AND ((`order_id`=?) OR (`user_id`=?));",
					Args:       []any{123, 12, 234},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and all",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id`=123) OR ((`user_id`=181) AND (`user_id`=234));",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));",
					Args:       []any{123, 181, 234},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ((`user_id`=123) OR (`user_id`=234)) AND (`order_id`=24);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 234, 24},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 234, 24},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and broadcast",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ((`user_id`=123) OR (`order_id`=12)) AND (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) AND (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ((`user_id`=123) OR (`user_id`=253)) OR (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or broadcast",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE ((`user_id`=123) OR (`order_id`=12)) OR (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_0",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-and",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ((`user_id`=123) AND (`order_id`=12)) AND (`order_id`=23);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 12, 23},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or-and",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE ((`user_id`=123) AND (`order_id`=12)) OR ((`user_id`=234) AND (`order_id`=18));",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args:       []any{123, 12, 234, 18},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args:       []any{123, 12, 234, 18},
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
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<?;"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{1},
								DB:         dbName,
								Datasource: dsName,
							})
						}
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
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<=?;"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{1},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where eq and lt",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id`=12) AND (`user_id`<133);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id`=?) AND (`user_id`<?);",
					Args:       []any{12, 133},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where gt",
			sql:  "SELECT `order_id`,`content` FROM order WHERE `user_id`>1;",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>?;"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{1},
								DB:         dbName,
								Datasource: dsName,
							})
						}
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
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>=?;"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{1},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where eq and gt",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id`=12) AND (`user_id`>133);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id`=?) AND (`user_id`>?);",
					Args:       []any{12, 133},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where eq and lt or gt",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE ((`user_id`=12) AND (`user_id`<133)) OR (`user_id`>234);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE ((`user_id`=?) AND (`user_id`<?)) OR (`user_id`>?);"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{12, 133, 234},
								DB:         dbName,
								Datasource: dsName,
							})
						}
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
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE `user_id` IN (?,?,?);",
					Args:       []any{12, 35, 101},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE `user_id` IN (?,?,?);",
					Args:       []any{12, 35, 101},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in and eq",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id` IN (12,35,101)) AND (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id` IN (?,?,?)) AND (`user_id`=?);",
					Args:       []any{12, 35, 101, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in or eq",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE (`user_id` IN (12,35,101)) OR (`user_id`=531);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{12, 35, 101, 531},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{12, 35, 101, 531},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{12, 35, 101, 531},
					DB:         "order_db_1",
					Datasource: "1.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in or gt",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` IN (12,35,101)) OR (`user_id`>531);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` IN (?,?,?)) OR (`user_id`>?);"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{12, 35, 101, 531},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where not in",
			sql:  "SELECT `order_id`,`content` FROM order WHERE `user_id` NOT IN (12,35,101);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id` NOT IN (?,?,?);"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{12, 35, 101},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where not in and eq",
			sql:  "SELECT `order_id`,`content` FROM order  WHERE (`user_id` NOT IN (12,35,101)) AND (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id` NOT IN (?,?,?)) AND (`user_id`=?);",
					Args:       []any{12, 35, 101, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not in or eq",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id` NOT IN (12,35,101)) OR (`user_id`=531);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (?,?,?)) OR (`user_id`=?);"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{12, 35, 101, 531},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where not in or gt",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id` NOT IN (12,35,101)) OR (`user_id`>531);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (?,?,?)) OR (`user_id`>?);"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{12, 35, 101, 531},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where not gt",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT (`user_id`>101);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`>?);"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{101},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where not lt",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT (`user_id`<101);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`<?);"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{101},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where not (gt and lt)",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ((`user_id`>12) AND (`user_id`<531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>?) AND (`user_id`<?));"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{12, 531},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where not (gt eq and lt eq)",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ((`user_id`>=12) AND (`user_id`<=531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>=?) AND (`user_id`<=?));"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{12, 531},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where not (in or gt)",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ((`user_id` IN (12,35,101)) OR (`user_id`>531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (?,?,?)) OR (`user_id`>?));"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{12, 35, 101, 531},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
		{
			name: "where not (in or eq)",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ((`user_id` IN (12,35,101)) OR (`user_id`=531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (?,?,?)) OR (`user_id`=?));"
				for a := 0; a < dsBase; a++ {
					dsName := fmt.Sprintf(dsPattern, a)
					for b := 0; b < dbBase; b++ {
						dbName := fmt.Sprintf(dbPattern, b)
						for c := 0; c < tableBase; c++ {
							tableName := fmt.Sprintf(tablePattern, c)
							res = append(res, sharding.Query{
								SQL:        fmt.Sprintf(sql, dbName, tableName),
								Args:       []any{12, 35, 101, 531},
								DB:         dbName,
								Datasource: dsName,
							})
						}
					}
				}
				return res
			}(),
		},
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
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_0` WHERE `user_id`=?;",
					Args:       []any{123},
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
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_0` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_1` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_0`.`order_tab_2` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_0` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_1` WHERE `order_id`=?;",
					Args:       []any{123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `user_id`,`order_id`,`content`,`account` FROM `order_db_1`.`order_tab_2` WHERE `order_id`=?;",
					Args:       []any{123},
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
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`order_id`=?) AND (`user_id`=?);",
					Args:       []any{12, 123},
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
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args:       []any{123, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id`=?) OR (`user_id`=?);",
					Args:       []any{123, 234},
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
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE (`order_id`=?) OR (`user_id`=?);",
					Args:       []any{12, 123},
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
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE (`user_id`=?) OR (`order_id`=?);",
					Args:       []any{123, 12},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or all",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ((`user_id`=123) AND (`order_id`=12)) OR (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id`=123) AND ((`order_id`=12) OR (`user_id`=234));",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) AND ((`order_id`=?) OR (`user_id`=?));",
					Args:       []any{123, 12, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and all",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id`=123) OR ((`user_id`=181) AND (`user_id`=234));",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id`=?) OR ((`user_id`=?) AND (`user_id`=?));",
					Args:       []any{123, 181, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ((`user_id`=123) OR (`user_id`=234)) AND (`order_id`=24);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 234, 24},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 234, 24},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-and broadcast",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ((`user_id`=123) OR (`order_id`=12)) AND (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) AND (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ((`user_id`=123) OR (`user_id`=253)) OR (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) OR (`user_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 253, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where or-or broadcast",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ((`user_id`=123) OR (`order_id`=12)) OR (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_1` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_2` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_1` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE ((`user_id`=?) OR (`order_id`=?)) OR (`user_id`=?);",
					Args:       []any{123, 12, 234},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-and",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ((`user_id`=123) AND (`order_id`=12)) AND (`order_id`=23);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) AND (`order_id`=?);",
					Args:       []any{123, 12, 23},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where and-or-and",
			sql:  "SELECT `order_id`,`content` FROM order WHERE ((`user_id`=123) AND (`order_id`=12)) OR ((`user_id`=234) AND (`order_id`=18));",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args:       []any{123, 12, 234, 18},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE ((`user_id`=?) AND (`order_id`=?)) OR ((`user_id`=?) AND (`order_id`=?));",
					Args:       []any{123, 12, 234, 18},
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
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<?;"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{1},
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
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`<=?;"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{1},
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
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>?;"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{1},
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
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id`>=?;"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{1},
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
			sql:  "SELECT `order_id`,`content` FROM order WHERE ((`user_id`=12) AND (`user_id`<133)) OR (`user_id`>234);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE ((`user_id`=?) AND (`user_id`<?)) OR (`user_id`>?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 133, 234},
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
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE `user_id` IN (?,?,?);",
					Args:       []any{12, 35, 101},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE `user_id` IN (?,?,?);",
					Args:       []any{12, 35, 101},
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
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id` IN (?,?,?)) AND (`user_id`=?);",
					Args:       []any{12, 35, 101, 234},
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
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{12, 35, 101, 531},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_0` WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{12, 35, 101, 531},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE (`user_id` IN (?,?,?)) OR (`user_id`=?);",
					Args:       []any{12, 35, 101, 531},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where in or gt",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` IN (12,35,101)) OR (`user_id`>531);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` IN (?,?,?)) OR (`user_id`>?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 35, 101, 531},
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
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE `user_id` NOT IN (?,?,?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 35, 101},
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
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id` NOT IN (12,35,101)) AND (`user_id`=234);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_0`.`order_tab_0` WHERE (`user_id` NOT IN (?,?,?)) AND (`user_id`=?);",
					Args:       []any{12, 35, 101, 234},
					DB:         "order_db_0",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not in or eq",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (12,35,101)) OR (`user_id`=531);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (?,?,?)) OR (`user_id`=?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 35, 101, 531},
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
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (12,35,101)) OR (`user_id`>531);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id` NOT IN (?,?,?)) OR (`user_id`>?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 35, 101, 531},
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
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`>101);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`>?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{101},
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
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT (`user_id`<101);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`<?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{101},
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
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT (`user_id`>=101);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`>=?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{101},
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
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT (`user_id`<=101);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`<=?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{101},
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
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`=101);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT (`user_id`=?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{101},
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
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT (`user_id`!=101);",
			qs: []sharding.Query{
				{
					SQL:        "SELECT `order_id`,`content` FROM `order_db_1`.`order_tab_2` WHERE NOT (`user_id`!=?);",
					Args:       []any{101},
					DB:         "order_db_1",
					Datasource: "0.db.cluster.company.com:3306",
				},
			},
		},
		{
			name: "where not (gt and lt)",
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>12) AND (`user_id`<531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>?) AND (`user_id`<?));"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 531},
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
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ((`user_id`>=12) AND (`user_id`<=531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`>=?) AND (`user_id`<=?));"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 531},
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
			sql:  "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (12,35,101)) OR (`user_id`>531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (?,?,?)) OR (`user_id`>?));"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 35, 101, 531},
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
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ((`user_id` IN (12,35,101)) OR (`user_id`=531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id` IN (?,?,?)) OR (`user_id`=?));"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 35, 101, 531},
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
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ((`user_id`=12) AND (`user_id`=531));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`=?) AND (`user_id`=?));"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 531},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where not (eq and eq not sharding key)",
			sql:  "SELECT `order_id`,`content` FROM order WHERE NOT ((`user_id`=12) AND (`order_id`=111));",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE NOT ((`user_id`=?) AND (`order_id`=?));"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 111},
							DB:         dbName,
							Datasource: dsPattern,
						})
					}
				}
				return res
			}(),
		},
		{
			name: "where between",
			sql:  "SELECT `order_id`,`content` FROM order WHERE (`user_id`>=12) AND (`user_id`<=531);",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s` WHERE (`user_id`>=?) AND (`user_id`<=?);"
				for i := 0; i < dbBase; i++ {
					dbName := fmt.Sprintf(dbPattern, i)
					for j := 0; j < tableBase; j++ {
						tableName := fmt.Sprintf(tablePattern, j)
						res = append(res, sharding.Query{
							SQL:        fmt.Sprintf(sql, dbName, tableName),
							Args:       []any{12, 531},
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
				sql := "SELECT `order_id`,`content` FROM `%s`.`%s`;"
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
			name: "select 列中带有聚合函数COUNT(*)",
			sql:  "select count(*) from order ",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT COUNT(*) FROM `%s`.`%s`;"
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
			name: "select 列中带有聚合函数COUNT(1)",
			sql:  "select count(1) from order ",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT COUNT(1) FROM `%s`.`%s`;"
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
			name: "聚合函数AVG(`id`)",
			sql:  "select AVG(`id`) from order ",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT SUM(`id`),COUNT(`id`) FROM `%s`.`%s`;"
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
			name: "Distinct单个字段",
			sql:  "select distinct id from order;",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT DISTINCT `id` FROM `%s`.`%s`;"
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
			name: "聚合函数和distinct",
			sql:  "select Avg(distinct id) from order;",
			qs: func() []sharding.Query {
				var res []sharding.Query
				sql := "SELECT SUM(DISTINCT id),COUNT(DISTINCT id) FROM `%s`.`%s`;"
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
			sql:  "select order_id,item_id,using_col1,using_col2 from order_detail where (order_id=123) or (order_id = 234);",
			mockOrder: func(mock1, mock2 sqlmock.Sqlmock) {
				rows1 := mock1.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"})
				rows1.AddRow(234, 12, "Kevin", "Durant")
				mock1.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_0`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);").
					WithArgs(123, 234).WillReturnRows(rows1)
				rows2 := mock2.NewRows([]string{"order_id", "item_id", "using_col1", "using_col2"})
				rows2.AddRow(123, 10, "LeBron", "James")
				mock2.ExpectQuery("SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail_db_1`.`order_detail_tab_0` WHERE (`order_id`=?) OR (`order_id`=?);").
					WithArgs(123, 234).WillReturnRows(rows2)
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
			row, err := handler.GetMulti(context.Background())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			res := make([]*OrderDetail, 0, 16)
			for row.Next() {
				d := &OrderDetail{}
				err = row.Scan(&d.OrderId, &d.ItemId, &d.UsingCol1, &d.UsingCol2)
				require.NoError(t, err)
				res = append(res, d)
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
