package vparser

import (
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/stretchr/testify/assert"
)

func TestHintVisitor(t *testing.T) {
	testcases := []struct {
		name    string
		sql     string
		wantVal string
	}{
		{
			name:    "mysql的SELECT语句支持的hint语法",
			sql:     "SELECT /* useMaster */   * FROM users WHERE (user_id = 1) or (user_id =2);",
			wantVal: "/* useMaster */",
		},
		{
			name:    "mysql的其他语句暂不支持hint语法",
			sql:     "DELETE FROM users WHERE (user_id = 1) or (user_id =2);",
			wantVal: "当前SQL语句尚不支持Hint语法",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			root := ast.Parse(tc.sql)
			hint := NewHintVisitor().Visit(root)
			assert.Equal(t, tc.wantVal, hint)
		})
	}
}
