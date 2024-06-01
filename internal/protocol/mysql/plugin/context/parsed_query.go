package pcontext

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
)

// ParsedQuery 代表一个经过了 AST 解析的查询
type ParsedQuery struct {
	Root     parser.IRootContext
}

// FirstDML 第一个 DML 语句，也就是增删改查语句。
// 我们会认为必然有一个语句，参考 parser 里面的定义，你就能理解。
func (q *ParsedQuery) FirstDML() *parser.DmlStatementContext {
	sqlStmt := q.FirstStatement()
	dmlStmt := sqlStmt.GetChildren()[0]
	return dmlStmt.(*parser.DmlStatementContext)
}

func (q *ParsedQuery) FirstStatement() *parser.SqlStatementContext {
	sqlStmts := q.Root.GetChildren()[0]
	sqlStmt := sqlStmts.GetChildren()[0]
	return sqlStmt.(*parser.SqlStatementContext)
}
