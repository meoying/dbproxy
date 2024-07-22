package vparser

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
)

const (
	SelectStmt           = "select"
	UpdateStmt           = "update"
	DeleteStmt           = "delete"
	InsertStmt           = "insert"
	StartTransactionStmt = "startTransaction"
	CommitStmt           = "commit"
	RollbackStmt         = "rollback"
	UnKnownSQLStmt       = "未知的SQL语句"
)

// CheckVisitor 用于判断SQL语句的类型/特征
type CheckVisitor struct {
	SqlType string
	parser.BaseMySqlParserVisitor
}

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
	return c.VisitSqlStatement(sqlStmt.(*parser.SqlStatementContext))
}

func (c *CheckVisitor) VisitSqlStatement(ctx *parser.SqlStatementContext) any {
	switch {
	case ctx.DmlStatement() != nil:
		return c.VisitDmlStatement(ctx.DmlStatement().(*parser.DmlStatementContext))
	case ctx.TransactionStatement() != nil:
		return c.VisitTransactionStatement(ctx.TransactionStatement().(*parser.TransactionStatementContext))
	default:
		return UnKnownSQLStmt
	}
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
		return UnKnownSQLStmt
	}
}

func (c *CheckVisitor) VisitTransactionStatement(ctx *parser.TransactionStatementContext) any {
	switch ctx.GetChildren()[0].(type) {
	case *parser.StartTransactionContext:
		return StartTransactionStmt
	case *parser.CommitWorkContext:
		return CommitStmt
	case *parser.RollbackWorkContext:
		return RollbackStmt
	default:
		return UnKnownSQLStmt
	}
}
