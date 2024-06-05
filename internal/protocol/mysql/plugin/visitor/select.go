package visitor

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
)

type SelectVal struct {
	Cols      []Column
	Predicate Predicate
}

type SelectVisitor struct {
	*BaseVisitor
}

func (s *SelectVisitor) Name() string {
	return "SelectVisitor"
}

func NewsSelectVisitor() Visitor {
	return &SelectVisitor{
		BaseVisitor: &BaseVisitor{},
	}
}

func (s *SelectVisitor) Visit(tree antlr.ParseTree) any {
	ctx := tree.(*parser.RootContext)
	return s.VisitRoot(ctx)
}

func (s *SelectVisitor) VisitRoot(ctx *parser.RootContext) any {
	sqlStmts := ctx.GetChildren()[0]
	sqlStmt := sqlStmts.GetChildren()[0]
	stmtctx := sqlStmt.(*parser.SqlStatementContext)
	return s.VisitSqlStatement(stmtctx)
}

func (s *SelectVisitor) VisitSqlStatement(ctx *parser.SqlStatementContext) any {
	dmstmt := ctx.DmlStatement().(*parser.DmlStatementContext)
	return s.VisitDmlStatement(dmstmt)
}

func (s *SelectVisitor) VisitDmlStatement(ctx *parser.DmlStatementContext) any {
	selectStmtCtx := ctx.SelectStatement().(*parser.SimpleSelectContext)
	return s.VisitSimpleSelect(selectStmtCtx)
}

func (s *SelectVisitor) VisitSimpleSelect(ctx *parser.SimpleSelectContext) any {
	queryCtx := ctx.QuerySpecification()
	cols := s.VisitSelectElements(queryCtx.SelectElements().(*parser.SelectElementsContext))
	pre := s.VisitFromClause(queryCtx.FromClause().(*parser.FromClauseContext))
	return BaseVal{
		Data: SelectVal{
			Cols:      cols.([]Column),
			Predicate: pre.(Predicate),
		},
	}
}

func (s *SelectVisitor) VisitFromClause(ctx *parser.FromClauseContext) any {
	if ctx.WHERE() == nil {
		return Predicate{}
	}
	return s.visitWhere(ctx.Expression())
}

// 不处理join查询和子查询
func (s *SelectVisitor) VisitTableSources(ctx *parser.TableSourcesContext) any {
	tableCtx := ctx.GetChild(0).(*parser.AtomTableItemContext).TableName()
	return s.BaseVisitor.VisitTableName(tableCtx.(*parser.TableNameContext))
}

func (s *SelectVisitor) VisitSelectElements(ctx *parser.SelectElementsContext) any {
	colEles := ctx.GetChildren()
	cols := make([]Column, 0, len(colEles))
	for _, colEle := range colEles {
		if v, ok := colEle.(*parser.SelectColumnElementContext); ok {
			va := v.FullColumnName().GetText()
			if va == "*" {
				break
			}
			col := Column{
				Name: s.BaseVisitor.removeQuote(va),
			}
			if v.AS() != nil {
				col.Alias = v.Uid().GetText()
				cols = append(cols, col)
			} else {
				cols = append(cols, col)
			}
		}
	}
	return cols
}
