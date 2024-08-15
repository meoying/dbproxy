package pcontext

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/vparser"
)

// ParsedQuery 代表一个经过了 AST 解析的查询
type ParsedQuery struct {
	root parser.IRootContext
	// typeName 表示SQL询语句的类型名
	typeName string
	// TODO: 在这里把 Hint 放好，在解析 Root 的地方就解析出来放好（这可以认为是一个统一的机制）
	hintVisitor *vparser.HintVisitor
	hints       map[string]vparser.HintValue
}

func NewParsedQuery(query string, hintVisitor *vparser.HintVisitor) ParsedQuery {
	return ParsedQuery{
		root:        ast.Parse(query),
		hintVisitor: hintVisitor,
	}
}

func (q *ParsedQuery) Root() parser.IRootContext {
	return q.root
}

func (q *ParsedQuery) Type() string {
	if q.typeName == "" {
		q.typeName = vparser.NewCheckVisitor().Visit(q.root).(string)
	}
	return q.typeName
}

func (q *ParsedQuery) Hints() map[string]vparser.HintValue {
	if q.hints == nil {
		q.hints = q.parseHints()
	}
	return q.hints
}

func (q *ParsedQuery) parseHints() map[string]vparser.HintValue {
	// 当前只有SELECT语句支持hint语法
	return q.hintVisitor.VisitRoot(q.root.(*parser.RootContext)).(map[string]vparser.HintValue)
}

// FirstDML 第一个 DML 语句，也就是增删改查语句。
// 我们会认为必然有一个语句，参考 parser 里面的定义，你就能理解。
func (q *ParsedQuery) FirstDML() *parser.DmlStatementContext {
	sqlStmt := q.FirstStatement()
	dmlStmt := sqlStmt.GetChildren()[0]
	return dmlStmt.(*parser.DmlStatementContext)
}

func (q *ParsedQuery) SqlStatement() any {
	sqlStmt := q.FirstStatement()
	return sqlStmt.GetChildren()[0]
}

func (q *ParsedQuery) FirstStatement() *parser.SqlStatementContext {
	sqlStmts := q.root.GetChildren()[0]
	sqlStmt := sqlStmts.GetChildren()[0]
	return sqlStmt.(*parser.SqlStatementContext)
}
