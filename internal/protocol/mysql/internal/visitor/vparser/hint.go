package vparser

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
)

type HintVisitor struct {
	*BaseVisitor
}

func (s *HintVisitor) Name() string {
	return "HintVisitor"
}

func NewHintVisitor() *HintVisitor {
	return &HintVisitor{
		BaseVisitor: &BaseVisitor{},
	}
}

func (s *HintVisitor) Visit(tree antlr.ParseTree) any {
	ctx := tree.(*parser.RootContext)
	return s.VisitRoot(ctx)
}

func (s *HintVisitor) VisitRoot(ctx *parser.RootContext) any {
	sqlStmts := ctx.GetChildren()[0]
	sqlStmt := sqlStmts.GetChildren()[0]
	return s.VisitSqlStatement(sqlStmt.(*parser.SqlStatementContext))
}

func (s *HintVisitor) VisitSqlStatement(ctx *parser.SqlStatementContext) any {
	return s.VisitDmlStatement(ctx.DmlStatement().(*parser.DmlStatementContext))
}

func (s *HintVisitor) VisitDmlStatement(ctx *parser.DmlStatementContext) any {
	return s.VisitSimpleSelect(ctx.SelectStatement().(*parser.SimpleSelectContext))
}

func (s *HintVisitor) VisitSimpleSelect(ctx *parser.SimpleSelectContext) any {
	queryCtx := ctx.QuerySpecification()
	if queryCtx.ProxyHint() != nil {
		return queryCtx.ProxyHint().GetText()
	}
	return ""
}
