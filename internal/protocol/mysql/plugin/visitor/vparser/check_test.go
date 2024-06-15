package vparser

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestVisitor_Check(t *testing.T) {
	testcases := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "select语句",
			sql:  "select * from t1;",
			want: "select",
		},
		{
			name: "update语句",
			sql:  "UPDATE users SET email = 'new_email@example.com', age = 30 WHERE user_id = 1;",
			want: "update",
		},
		{
			name: "insert语句",
			sql:  "INSERT INTO employees (employee_id, first_name, last_name, salary) VALUES (101, 'John', 'Doe', 5000);",
			want: "insert",
		},
		{
			name: "delete语句",
			sql:  "DELETE FROM users WHERE age > 30;",
			want: "delete",
		},
		{
			name: "非dml语句",
			sql:  "ALTER TABLE employees ADD COLUMN birthdate DATE;",
			want: "",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			root := ast.Parse(tc.sql)
			checkV := &CheckVisitor{}
			strany := checkV.VisitRoot(root.(*parser.RootContext))
			str, ok := strany.(string)
			require.True(t, ok)
			assert.Equal(t, tc.want, str)
		})
	}

}
