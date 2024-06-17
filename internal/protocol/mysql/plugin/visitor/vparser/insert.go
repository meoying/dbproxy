package vparser

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor"
)

type ValMap map[string]any

type InsertVal struct {
	Vals []ValMap
	Cols []string
	// 复用这部分数据，减少创建ctx操作
	AstValues []*parser.ExpressionsWithDefaultsContext
	TableName string
}

type InsertVisitor struct {
	*BaseVisitor
}

func NewInsertVisitor() SqlParser {
	return &InsertVisitor{
		BaseVisitor: &BaseVisitor{},
	}
}

func (s *InsertVisitor) Parse(ctx antlr.ParseTree) any {
	return s.Visit(ctx)
}

func (s *InsertVisitor) Visit(tree antlr.ParseTree) any {
	ctx := tree.(*parser.RootContext)
	return s.VisitRoot(ctx)
}

func (i *InsertVisitor) Name() string {
	return "InsertVisitor"
}

func (i *InsertVisitor) VisitRoot(ctx *parser.RootContext) any {
	sqlStmts := ctx.GetChildren()[0]
	sqlStmt := sqlStmts.GetChildren()[0]
	stmtctx := sqlStmt.(*parser.SqlStatementContext)
	return i.VisitSqlStatement(stmtctx)
}

func (i *InsertVisitor) VisitSqlStatement(ctx *parser.SqlStatementContext) any {
	dmstmt := ctx.DmlStatement().(*parser.DmlStatementContext)
	return i.VisitDmlStatement(dmstmt)
}

func (i *InsertVisitor) VisitDmlStatement(ctx *parser.DmlStatementContext) any {
	insertStmt := ctx.InsertStatement().(*parser.InsertStatementContext)
	return i.VisitInsertStatement(insertStmt)
}

func (i *InsertVisitor) VisitInsertStatement(ctx *parser.InsertStatementContext) any {
	if ctx == nil {
		return BaseVal{
			Err: errStmtMatch,
		}
	}
	iVal := InsertVal{
		TableName: i.VisitTableName(ctx.TableName().(*parser.TableNameContext)).(string),
		Cols:      i.columns(ctx),
	}


	insertCtx := ctx.InsertStatementValue().(*parser.InsertStatementValueContext)
	if insertCtx.VALUES() == nil && insertCtx.VALUE() == nil {
		return BaseVal{
			Err: errStmtMatch,
		}
	}
	vv,astVals,err := i.visitInsertStatementValue(insertCtx, iVal.Cols)
	if err != nil {
		return BaseVal{
			Err: err,
		}
	}
	iVal.Vals = vv
	iVal.AstValues = astVals
	return BaseVal{
		Data: iVal,
	}
}

func (i *InsertVisitor) visitInsertStatementValue(ctx *parser.InsertStatementValueContext, cols []string) ([]ValMap,[]*parser.ExpressionsWithDefaultsContext, error) {
	exPressCtxs := ctx.AllExpressionsWithDefaults()
	ans := make([]ValMap, 0, len(exPressCtxs))
	astValues := make([]*parser.ExpressionsWithDefaultsContext,0,len(exPressCtxs))
	for _, expressCtx := range exPressCtxs {
		eCtx := expressCtx.(*parser.ExpressionsWithDefaultsContext)
		v, err := i.visitExpressionsWithDefaults(eCtx, cols)
		if err != nil {
			return nil,nil, err
		}
		astValues = append(astValues,eCtx)
		ans = append(ans, v)
	}
	return ans,astValues, nil
}

func (i *InsertVisitor) visitExpressionsWithDefaults(ctx *parser.ExpressionsWithDefaultsContext, cols []string) (ValMap, error) {
	res := ValMap{}
	ivals := ctx.AllExpressionOrDefault()
	if len(cols) != len(ivals) {
		return nil, errQueryInvalid
	}
	for idx, ival := range ivals {
		v := i.VisitExpressionOrDefault(ival.(*parser.ExpressionOrDefaultContext))
		res[cols[idx]] = v
	}
	return res, nil
}

func (i *InsertVisitor) VisitExpressionOrDefault(ctx *parser.ExpressionOrDefaultContext) any {
	val := i.BaseVisitor.VisitPredicateExpression(ctx.Expression().(*parser.PredicateExpressionContext))
	return val.(visitor.ValueExpr).Val
}

func (i *InsertVisitor) columns(insertStmt parser.IInsertStatementContext) []string {
	cols := make([]string, 0, 16)
	if insertStmt.FullColumnNameList() != nil {
		columnStmts := insertStmt.FullColumnNameList().AllFullColumnName()
		for _, colStmt := range columnStmts {
			cols = append(cols, i.RemoveQuote(colStmt.Uid().GetText()))
		}
	}
	return cols
}
