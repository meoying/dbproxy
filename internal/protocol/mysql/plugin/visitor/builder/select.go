package builder

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
)

type Select struct {
	*Base
}

func NewSelect(db, tab string) *Select {
	return &Select{
		Base: &Base{
			db:                     db,
			tab:                    tab,
			BaseMySqlParserVisitor: &parser.BaseMySqlParserVisitor{},
		},
	}
}

func (s *Select) Build(ctx antlr.ParseTree) (string, error) {
	err := s.VisitRoot(ctx.(*parser.RootContext))
	if err != nil {
		return "", err.(error)
	}
	v := s.removeEof(ctx.GetText())
	return v, nil
}

func (s *Select) VisitRoot(ctx *parser.RootContext) any {
	sqlStmts := ctx.GetChildren()[0]
	sqlStmt := sqlStmts.GetChildren()[0]
	stmtctx := sqlStmt.(*parser.SqlStatementContext)
	return s.VisitSqlStatement(stmtctx)
}

func (s *Select) VisitSqlStatement(ctx *parser.SqlStatementContext) any {
	dmstmt := ctx.DmlStatement().(*parser.DmlStatementContext)
	return s.VisitDmlStatement(dmstmt)
}

func (s *Select) VisitDmlStatement(ctx *parser.DmlStatementContext) any {
	selectStmtCtx := ctx.SelectStatement().(*parser.SimpleSelectContext)
	return s.VisitSimpleSelect(selectStmtCtx)
}

func (s *Select) VisitSimpleSelect(ctx *parser.SimpleSelectContext) any {
	queryCtx := ctx.QuerySpecification()
	return s.VisitFromClause(queryCtx.FromClause().(*parser.FromClauseContext))
}

func (s *Select) VisitFromClause(ctx *parser.FromClauseContext) any {
	return s.VisitTableSources(ctx.TableSources().(*parser.TableSourcesContext))
}

func (s *Select) VisitTableSources(ctx *parser.TableSourcesContext) any {
	tableSourceCtx := ctx.TableSource(0)
	return s.VisitTableSourceBase(tableSourceCtx.(*parser.TableSourceBaseContext))
}

func (s *Select) VisitTableSourceBase(ctx *parser.TableSourceBaseContext) any {
	return s.VisitAtomTableItem(ctx.TableSourceItem().(*parser.AtomTableItemContext))
}

func (s *Select) VisitAtomTableItem(ctx *parser.AtomTableItemContext) any {
	return s.VisitTableName(ctx.TableName().(*parser.TableNameContext))
}

