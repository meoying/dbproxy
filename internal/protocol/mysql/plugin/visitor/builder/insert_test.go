package builder

import (
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor/vparser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsert_Build(t *testing.T) {
	testcases := []struct {
		name      string
		valueFunc func(string) SqlBuilder
		sql       string
		wantSql   string
	}{
		{
			name: "修改多个值",
			sql:  "INSERT INTO `user` (username, email) VALUES ('john_doe','john@example.com'),('zwl','zwl@qq.com'),('dm','dm.@163.com');",
			valueFunc: func(sql string) SqlBuilder {
				root := ast.Parse(sql)
				baseVal := vparser.NewInsertVisitor().Parse(root).(vparser.BaseVal)
				require.NoError(t, baseVal.Err)
				insertVal := baseVal.Data.(vparser.InsertVal)
				assert.Equal(t, 3, len(insertVal.AstValues))
				return NewInsert("user_db_1", "user_tab_1", []*parser.ExpressionsWithDefaultsContext{
					insertVal.AstValues[0],
					insertVal.AstValues[2],
				})
			},
			wantSql: "INSERT INTO `user_db_1`.`user_tab_1` ( username , email ) VALUES ( 'john_doe','john@example.com' ) , ( 'dm','dm.@163.com' ) ; ",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			root := ast.Parse(tc.sql)
			builder := tc.valueFunc(tc.sql)
			sql, err := builder.Build(root)
			require.NoError(t, err)
			assert.Equal(t, tc.wantSql, sql)
		})
	}

}
