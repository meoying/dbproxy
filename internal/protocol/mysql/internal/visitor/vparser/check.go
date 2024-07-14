package vparser

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
)

// 用于判断

type CheckVisitor struct {
	SqlType string
	parser.BaseMySqlParserVisitor
}

const (
	SelectStmt     = "select"
	UpdateStmt     = "update"
	DeleteStmt     = "delete"
	InsertStmt     = "insert"
	UnKnownDMLStmt = "未知的DML语句"
	UnKnownSQLStmt = "未知的SQL语句"
)

func NewCheckVisitor() *CheckVisitor {
	return &CheckVisitor{}
}

func (c *CheckVisitor) Name() string {
	return "CheckVisitor"
}

func (c *CheckVisitor) Visit(tree antlr.ParseTree) any {
	ctx := tree.(*parser.RootContext)
	return c.VisitRoot(ctx)
}

func (c *CheckVisitor) VisitRoot(ctx *parser.RootContext) any {
	sqlStmts := ctx.GetChildren()[0]
	sqlStmt := sqlStmts.GetChildren()[0]
	stmtctx := sqlStmt.(*parser.SqlStatementContext)
	return c.VisitSqlStatement(stmtctx)
}

func (c *CheckVisitor) VisitSqlStatement(ctx *parser.SqlStatementContext) any {
	dmstmt, ok := ctx.DmlStatement().(*parser.DmlStatementContext)
	if !ok {
		return UnKnownSQLStmt
	}
	return c.VisitDmlStatement(dmstmt)
}

func (c *CheckVisitor) VisitDmlStatement(ctx *parser.DmlStatementContext) any {
	switch {
	case ctx.InsertStatement() != nil:
		return InsertStmt
	case ctx.SelectStatement() != nil:
		return SelectStmt
	case ctx.UpdateStatement() != nil:
		return UpdateStmt
	case ctx.DeleteStatement() != nil:
		return DeleteStmt
	default:
		return UnKnownDMLStmt
	}
}
