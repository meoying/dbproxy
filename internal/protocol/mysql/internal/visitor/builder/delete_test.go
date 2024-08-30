package builder

import (
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/stretchr/testify/assert"
)

func TestDeleteBuilder_Build(t *testing.T) {
	testcases := []struct {
		name          string
		sql           string
		deleteBuilder *Delete
		wantSql       string
		wantErr       error
	}{
		{
			name:          "替换表名",
			sql:           "DELETE FROM t1 WHERE id = 1;",
			deleteBuilder: NewDelete("order_db_1", "order_tab_1"),
			wantSql:       "DELETE FROM `order_db_1`.`order_tab_1` WHERE id = 1 ; ",
		},
		{
			name:          "表名有`",
			sql:           "DELETE FROM `t1` WHERE id = 1;",
			deleteBuilder: NewDelete("order_db_0", "order_tab_1"),
			wantSql:       "DELETE FROM `order_db_0`.`order_tab_1` WHERE id = 1 ; ",
		},
		{
			name:          "表名为关键字",
			sql:           "DELETE FROM `order` WHERE id = 1;",
			deleteBuilder: NewDelete("order_db_0", "order_tab_1"),
			wantSql:       "DELETE FROM `order_db_0`.`order_tab_1` WHERE id = 1 ; ",
		},
		{
			name:          "原表名为 xx.xx的形式",
			sql:           "DELETE FROM order_db.tab WHERE id = 1;",
			deleteBuilder: NewDelete("order_db_0", "order_tab_1"),
			wantSql:       "DELETE FROM `order_db_0`.`order_tab_1` WHERE id = 1 ; ",
		},
		{
			name:          "只有表名，没有库名",
			sql:           "DELETE FROM `tab` WHERE id = 1;",
			deleteBuilder: NewDelete("", "order_tab_1"),
			wantSql:       "DELETE FROM `order_tab_1` WHERE id = 1 ; ",
		},
		{
			name:          "库名，表名都有`",
			sql:           "DELETE FROM `order_db`.`tab` WHERE id = 1;",
			deleteBuilder: NewDelete("order_db_0", "order_tab_1"),
			wantSql:       "DELETE FROM `order_db_0`.`order_tab_1` WHERE id = 1 ; ",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			root := ast.Parse(tc.sql).Root
			builder := tc.deleteBuilder
			sql, err := builder.Build(root)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantSql, sql)
		})
	}
}
