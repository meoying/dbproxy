package builder

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
)

type Delete struct {
	*Base
}

func NewDelete(db, tab string) *Delete {
	return &Delete{
		Base: &Base{
			db:  db,
			tab: tab,
		},
	}
}

func (d *Delete) Build(ctx antlr.ParseTree) (string, error) {
	err := d.VisitRoot(ctx.(*parser.RootContext))
	if err != nil {
		return "", err.(error)
	}
	sql := d.removeEof(ctx.GetText())
	return sql, nil
}

func (d *Delete) Visit(tree antlr.ParseTree) any {
	ctx := tree.(*parser.RootContext)
	return d.VisitRoot(ctx)
}

func (d *Delete) VisitRoot(ctx *parser.RootContext) any {
	sqlStmts := ctx.GetChildren()[0]
	sqlStmt := sqlStmts.GetChildren()[0]
	stmtctx := sqlStmt.(*parser.SqlStatementContext)
	return d.VisitSqlStatement(stmtctx)
}

func (d *Delete) VisitSqlStatement(ctx *parser.SqlStatementContext) any {
	dmstmt := ctx.DmlStatement().(*parser.DmlStatementContext)
	return d.VisitDmlStatement(dmstmt)
}

func (d *Delete) VisitDmlStatement(ctx *parser.DmlStatementContext) any {
	if ctx.DeleteStatement().SingleDeleteStatement() == nil {
		return errUnsupportedDeleteSql
	}
	deleteCtx := ctx.DeleteStatement().SingleDeleteStatement().(*parser.SingleDeleteStatementContext)
	return d.VisitSingleDeleteStatement(deleteCtx)
}

func (d *Delete) VisitSingleDeleteStatement(ctx *parser.SingleDeleteStatementContext) any {
	return d.VisitTableName(ctx.TableName().(*parser.TableNameContext))
}
