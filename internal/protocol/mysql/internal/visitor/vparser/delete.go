package vparser

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor"
)

type DeleteVal struct {
	Predicate visitor.Predicate
}

type DeleteVisitor struct {
	*BaseVisitor
}

func (s *DeleteVisitor) Parse(ctx antlr.ParseTree) any {
	return s.Visit(ctx)
}

func (s *DeleteVisitor) Name() string {
	return "DeleteVisitor"
}

func NewDeleteVisitor() SqlParser {
	return &DeleteVisitor{
		BaseVisitor: &BaseVisitor{},
	}
}

func (s *DeleteVisitor) Visit(tree antlr.ParseTree) any {
	ctx := tree.(*parser.RootContext)
	return s.VisitRoot(ctx)
}

func (s *DeleteVisitor) VisitRoot(ctx *parser.RootContext) any {
	sqlStmts := ctx.GetChildren()[0]
	sqlStmt := sqlStmts.GetChildren()[0]
	stmtctx := sqlStmt.(*parser.SqlStatementContext)
	return s.VisitSqlStatement(stmtctx)
}

func (s *DeleteVisitor) VisitSqlStatement(ctx *parser.SqlStatementContext) any {
	dmstmt := ctx.DmlStatement().(*parser.DmlStatementContext)
	return s.VisitDmlStatement(dmstmt)
}

func (s *DeleteVisitor) VisitDmlStatement(ctx *parser.DmlStatementContext) any {
	if ctx.DeleteStatement().SingleDeleteStatement() == nil {
		return BaseVal{
			Err: errUnsupportedDeleteSql,
		}
	}
	deleteCtx := ctx.DeleteStatement().SingleDeleteStatement().(*parser.SingleDeleteStatementContext)
	return s.VisitSingleDeleteStatement(deleteCtx)
}

func (s *DeleteVisitor) VisitSingleDeleteStatement(ctx *parser.SingleDeleteStatementContext) any {
	v := s.BaseVisitor.visitWhere(ctx.Expression())
	return BaseVal{
		Data: DeleteVal{
			Predicate: v.(visitor.Predicate),
		},
	}
}
