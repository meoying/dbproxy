package builder

import (
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/stretchr/testify/assert"
)

func TestUpdateBuilder_Build(t *testing.T) {
	testcases := []struct {
		name          string
		sql           string
		updateBuilder *Update
		wantSql       string
		wantErr       error
	}{
		{
			name:          "替换表名",
			sql:           "UPDATE t1 SET uid = 1  WHERE id = 1;",
			updateBuilder: NewUpdate("order_db_1", "order_tab_1"),
			wantSql:       "UPDATE `order_db_1`.`order_tab_1` SET uid = 1 WHERE id = 1 ; ",
		},
		{
			name:          "表名有`",
			sql:           "UPDATE `t1` SET uid = 1  WHERE id = 1;",
			updateBuilder: NewUpdate("order_db_0", "order_tab_1"),
			wantSql:       "UPDATE `order_db_0`.`order_tab_1` SET uid = 1 WHERE id = 1 ; ",
		},
		{
			name:          "表名为关键字",
			sql:           "UPDATE `order` SET uid = 1  WHERE id = 1;",
			updateBuilder: NewUpdate("order_db_0", "order_tab_1"),
			wantSql:       "UPDATE `order_db_0`.`order_tab_1` SET uid = 1 WHERE id = 1 ; ",
		},
		{
			name:          "原表名为 xx.xx的形式",
			sql:           "UPDATE order_db.tab SET uid = 1  WHERE id = 1;",
			updateBuilder: NewUpdate("order_db_0", "order_tab_1"),
			wantSql:       "UPDATE `order_db_0`.`order_tab_1` SET uid = 1 WHERE id = 1 ; ",
		},
		{
			name:          "只有表名，没有库名",
			sql:           "UPDATE `tab` SET uid = 1  WHERE id = 1;",
			updateBuilder: NewUpdate("", "order_tab_1"),
			wantSql:       "UPDATE `order_tab_1` SET uid = 1 WHERE id = 1 ; ",
		},
		{
			name:          "库名，表名都有`",
			sql:           "UPDATE `order_db`.`tab` SET uid = 1  WHERE id = 1;",
			updateBuilder: NewUpdate("order_db_0", "order_tab_1"),
			wantSql:       "UPDATE `order_db_0`.`order_tab_1` SET uid = 1 WHERE id = 1 ; ",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			root := ast.Parse(tc.sql).Root
			builder := tc.updateBuilder
			sql, err := builder.Build(root)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantSql, sql)
		})
	}
}
