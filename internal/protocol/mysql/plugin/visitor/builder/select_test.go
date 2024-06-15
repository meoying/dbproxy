package builder

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSelectBuilder_Build(t *testing.T) {
	testcases := []struct {
		name          string
		sql           string
		selectBuilder *Select
		wantSql       string
		wantErr       error
	}{
		{
			name:          "替换表名",
			sql:           "select * from t1 where id = 1;",
			selectBuilder: NewSelect("order_db_1", "order_tab_1"),
			wantSql:       "select * from `order_db_1`.`order_tab_1` where id = 1 ; ",
		},
		{
			name:          "表名有`",
			sql:           "select * from `order` where id = 1;",
			selectBuilder: NewSelect("order_db_0", "order_tab_1"),
			wantSql:       "select * from `order_db_0`.`order_tab_1` where id = 1 ; ",
		},
		{
			name:          "表名为关键字",
			sql:           "select * from order where id = 1;",
			selectBuilder: NewSelect("order_db_0", "order_tab_1"),
			wantSql:       "select * from `order_db_0`.`order_tab_1` where id = 1 ; ",
		},
		{
			name:          "原表名为 xx.xx的形式",
			sql:           "select * from db1.order where id = 1;",
			selectBuilder: NewSelect("order_db_0", "order_tab_1"),
			wantSql:       "select * from `order_db_0`.`order_tab_1` where id = 1 ; ",
		},
		{
			name:          "只有表名，没有库名",
			sql:           "select * from order where id = 1;",
			selectBuilder: NewSelect("", "order_tab_1"),
			wantSql:       "select * from `order_tab_1` where id = 1 ; ",
		},
		{
			name:          "库名，表名都有`",
			sql:           "select * from `order`.`tab` where id = 1;",
			selectBuilder: NewSelect("order_db_0", "order_tab_1"),
			wantSql:       "select * from `order_db_0`.`order_tab_1` where id = 1 ; ",
		},
		{
			name:          "替换聚合函数 avg(age)",
			sql:           "select avg(age) from order;",
			selectBuilder: NewSelect("order_db_0", "order_tab_1"),
			wantSql:       "select SUM(age),COUNT(age) from `order_db_0`.`order_tab_1` ; ",
		},
		{
			name:          "替换聚合函数 avg(distinct age)",
			sql:           "select avg(distinct age) from order;",
			selectBuilder: NewSelect("order_db_0", "order_tab_1"),
			wantSql:       "select SUM(distinct age),COUNT(distinct age) from `order_db_0`.`order_tab_1` ; ",
		},
		{
			name:          "替换聚合函数avg 带有as",
			sql:           "select avg(age) as avgAge from order;",
			selectBuilder: NewSelect("order_db_0", "order_tab_1"),
			wantSql:       "select SUM(age),COUNT(age) from `order_db_0`.`order_tab_1` ; ",
		},
		{
			name:          "替换多个聚合函数avg,",
			sql:           "select avg(age),avg(col2) as avgAge from order;",
			selectBuilder: NewSelect("order_db_0", "order_tab_1"),
			wantSql:       "select SUM(age),COUNT(age),SUM(col2),COUNT(col2) from `order_db_0`.`order_tab_1` ; ",
		},
		{
			name:          "一个聚合函数和多个列,",
			sql:           "select col,avg(age) as avgAge,col1 from order;",
			selectBuilder: NewSelect("order_db_0", "order_tab_1"),
			wantSql:       "select col,SUM(age),COUNT(age),col1 from `order_db_0`.`order_tab_1` ; ",
		},
		{
			name:          "存在 limit 替换 Limit 和offset",
			sql:           "select * from t1 limit 10;",
			selectBuilder: NewSelect("order_db_0", "order_tab_1", WithLimit(10, 1)),
			wantSql:       "select * from `order_db_0`.`order_tab_1` LIMIT 10 OFFSET 1 ; ",
		},
		{
			name:          "没有limit替换 Limit 和offset",
			sql:           "select * from t1;",
			selectBuilder: NewSelect("order_db_0", "order_tab_1", WithLimit(10, 1)),
			wantSql:       "select * from `order_db_0`.`order_tab_1` LIMIT 10 OFFSET 1 ; ",
		},
		{
			name:          "存在limit和offset 替换Limit和offset",
			sql:           "select * from t1 limit 10 offset 9 ;",
			selectBuilder: NewSelect("order_db_0", "order_tab_1", WithLimit(10, 3)),
			wantSql:       "select * from `order_db_0`.`order_tab_1` LIMIT 10 OFFSET 3 ; ",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			root := ast.Parse(tc.sql)
			builder := tc.selectBuilder
			sql, err := builder.Build(root)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantSql, sql)
		})
	}
}
