package visitor

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
	stmtctx := sqlStmt.(*parser.SqlStatementContext)
	return s.VisitSqlStatement(stmtctx)
}

func (s *HintVisitor) VisitSqlStatement(ctx *parser.SqlStatementContext) any {
	dmstmt := ctx.DmlStatement().(*parser.DmlStatementContext)
	return s.VisitDmlStatement(dmstmt)
}

func (s *HintVisitor) VisitDmlStatement(ctx *parser.DmlStatementContext) any {
	selectStmtCtx := ctx.SelectStatement().(*parser.SimpleSelectContext)
	return s.VisitSimpleSelect(selectStmtCtx)
}

func (s *HintVisitor) VisitSimpleSelect(ctx *parser.SimpleSelectContext) any {
	queryCtx := ctx.QuerySpecification()
	if queryCtx.ProxyHint() != nil {
		return queryCtx.ProxyHint().GetText()
	}
	return ""
}
