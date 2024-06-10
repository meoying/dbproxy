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
