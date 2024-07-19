package pcontext

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/vparser"
)

// ParsedQuery 代表一个经过了 AST 解析的查询
type ParsedQuery struct {
	Root parser.IRootContext
	// TODO: 在这里把 Hint 放好，在解析 Root 的地方就解析出来放好（这可以认为是一个统一的机制）
	Hints []string
	// typeName 表示SQL询语句的类型名
	typeName string
}

// NewParsedQuery
// TODO: 使用NewParsedQuery重构所有ParsedQuery直接初始化的代码,并将字段设置为包内可访问+提供方法
func NewParsedQuery(query string) *ParsedQuery {
	astRoot := ast.Parse(query)
	return &ParsedQuery{
		Root:     astRoot,
		typeName: vparser.NewCheckVisitor().Visit(astRoot).(string),
		Hints:    parseHints(astRoot),
	}
}

func parseHints(astRoot parser.IRootContext) []string {
	var hints []string
	visitor := vparser.NewHintVisitor()
	v := visitor.Visit(astRoot)
	if text, ok := v.(string); ok {
		hints = append(hints, text)
	}
	return hints
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
	sqlStmts := q.Root.GetChildren()[0]
	sqlStmt := sqlStmts.GetChildren()[0]
	return sqlStmt.(*parser.SqlStatementContext)
}

func (q *ParsedQuery) Type() string {
	return q.typeName
}
