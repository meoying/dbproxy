package builder

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
)

type Insert struct {
	*Base
	astValues []*parser.ExpressionsWithDefaultsContext
}

func NewInsert(db, tab string, astValues []*parser.ExpressionsWithDefaultsContext) SqlBuilder {
	return &Insert{
		Base: &Base{
			db:  db,
			tab: tab,
		},
		astValues: astValues,
	}
}

func (i *Insert) Build(ctx antlr.ParseTree) (string, error) {
	err := i.VisitRoot(ctx.(*parser.RootContext))
	if err != nil {
		return "", err.(error)
	}
	sql := i.removeEof(ctx.GetText())
	return sql, nil
}

func (i *Insert) VisitRoot(ctx *parser.RootContext) any {
	sqlStmts := ctx.GetChildren()[0]
	sqlStmt := sqlStmts.GetChildren()[0]
	stmtctx := sqlStmt.(*parser.SqlStatementContext)
	return i.VisitSqlStatement(stmtctx)
}

func (i *Insert) VisitSqlStatement(ctx *parser.SqlStatementContext) any {
	dmstmt := ctx.DmlStatement().(*parser.DmlStatementContext)
	return i.VisitDmlStatement(dmstmt)
}

func (i *Insert) VisitDmlStatement(ctx *parser.DmlStatementContext) any {
	insertStatementCtx := ctx.InsertStatement().(*parser.InsertStatementContext)
	return i.VisitInsertStatement(insertStatementCtx)
}

func (i *Insert) VisitInsertStatement(ctx *parser.InsertStatementContext) any {
	// 处理表名
	i.VisitTableName(ctx.TableName().(*parser.TableNameContext))
	// 处理值
	return i.VisitInsertStatementValue(ctx.InsertStatementValue().(*parser.InsertStatementValueContext))
}

func (i *Insert) VisitInsertStatementValue(ctx *parser.InsertStatementValueContext) any {
	// 将value或者values后面的所有数据都给删除
	childCount := ctx.GetChildCount()
	for i := 0; i < childCount-1; i++ {
		ctx.RemoveLastChild()
	}
	for idx, v := range i.astValues {
		if idx > 0 {
			// 逗号
			i.newComma(ctx)
		}
		// 左括号
		i.newLRBracket(ctx)
		// 值
		ctx.AddChild(v)
		// 右括号
		i.newRRBracket(ctx)
	}
	return nil
}
func (i *Insert) newLRBracket(ctx *parser.InsertStatementValueContext) {
	token := ctx.GetStop()
	lrToken := antlr.NewCommonToken(token.GetSource(), parser.MySqlParserLR_BRACKET, token.GetChannel(), token.GetStart(), token.GetStop())
	lrToken.SetText("(")
	ctx.AddTokenNode(lrToken)
}

func (i *Insert) newRRBracket(ctx *parser.InsertStatementValueContext) {
	token := ctx.GetStop()
	rrToken := antlr.NewCommonToken(token.GetSource(), parser.MySqlParserRR_BRACKET, token.GetChannel(), token.GetStart(), token.GetStop())
	rrToken.SetText(")")
	ctx.AddTokenNode(rrToken)
}

func (i *Insert) newComma(ctx *parser.InsertStatementValueContext) {
	token := ctx.GetStop()
	commaToken := antlr.NewCommonToken(token.GetSource(), parser.MySqlParserCOMMA, token.GetChannel(), token.GetStart(), token.GetStop())
	commaToken.SetText(",")
	ctx.AddTokenNode(commaToken)
}
