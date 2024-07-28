package vparser

import (
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/stretchr/testify/assert"
)

func TestVisitor_Check(t *testing.T) {
	testcases := []struct {
		name     string
		sql      string
		wantName string
	}{
		{
			name:     "select语句",
			sql:      "select * from t1;",
			wantName: SelectStmt,
		},
		{
			name:     "update语句",
			sql:      "UPDATE users SET email = 'new_email@example.com', age = 30 WHERE user_id = 1;",
			wantName: UpdateStmt,
		},
		{
			name:     "insert语句",
			sql:      "INSERT INTO employees (employee_id, first_name, last_name, salary) VALUES (101, 'John', 'Doe', 5000);",
			wantName: InsertStmt,
		},
		{
			name:     "delete语句",
			sql:      "DELETE FROM users WHERE age > 30;",
			wantName: DeleteStmt,
		},
		{
			name:     "开启事务语句",
			sql:      "START TRANSACTION;",
			wantName: StartTransactionStmt,
		},
		{
			name:     "提交事务语句",
			sql:      "COMMIT;",
			wantName: CommitStmt,
		},
		{
			name:     "回滚事务语句",
			sql:      "ROLLBACK;",
			wantName: RollbackStmt,
		},
		{
			name:     "创建Prepare语句",
			sql:      "PREPARE stmt1 FROM 'SELECT * FROM order;';",
			wantName: PrepareStmt,
		},
		{
			name:     "执行Prepare语句",
			sql:      "EXECUTE stmt1;",
			wantName: ExecutePrepareStmt,
		},
		{
			name:     "关闭Prepare语句",
			sql:      "DEALLOCATE PREPARE stmt1;",
			wantName: DeallocatePrepareStmt,
		},
		{
			name:     "未知支持的SQL语句",
			sql:      "ALTER TABLE employees ADD COLUMN birthdate DATE;",
			wantName: UnKnownSQLStmt,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			root := ast.Parse(tc.sql)
			checkV := &CheckVisitor{}
			v := checkV.VisitRoot(root.(*parser.RootContext))
			actualName, ok := v.(string)
			assert.True(t, ok)
			assert.Equal(t, tc.wantName, actualName)
		})
	}

}
