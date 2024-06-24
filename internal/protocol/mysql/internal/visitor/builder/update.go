package builder

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
)

type Update struct {
	*Base
}

func NewUpdate(db, tab string) *Update {
	return &Update{
		Base: &Base{
			db:  db,
			tab: tab,
		},
	}
}
func (u *Update) Build(ctx antlr.ParseTree) (string, error) {
	err := u.VisitRoot(ctx.(*parser.RootContext))
	if err != nil {
		return "", err.(error)
	}
	sql := u.removeEof(ctx.GetText())
	return sql, nil
}

func (u *Update) VisitRoot(ctx *parser.RootContext) any {
	sqlStmts := ctx.GetChildren()[0]
	sqlStmt := sqlStmts.GetChildren()[0]
	stmtctx := sqlStmt.(*parser.SqlStatementContext)
	return u.VisitSqlStatement(stmtctx)
}

func (u *Update) VisitSqlStatement(ctx *parser.SqlStatementContext) any {
	dmstmt := ctx.DmlStatement().(*parser.DmlStatementContext)
	return u.VisitDmlStatement(dmstmt)
}

func (u *Update) VisitDmlStatement(ctx *parser.DmlStatementContext) any {
	updateStatementCtx := ctx.UpdateStatement()
	if updateStatementCtx.SingleUpdateStatement() == nil {
		return errUnsupportedUpdateSql
	}

	return u.VisitSingleUpdateStatement(updateStatementCtx.SingleUpdateStatement().(*parser.SingleUpdateStatementContext))
}

func (u *Update) VisitSingleUpdateStatement(ctx *parser.SingleUpdateStatementContext) any {
	return u.VisitTableName(ctx.TableName().(*parser.TableNameContext))
}
