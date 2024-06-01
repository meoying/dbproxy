package visitor

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"strings"
)

type ValMap map[string]*ValMeta

type InsertVal struct {
	Vals      []ValMap
	Cols      []string
	TableName string
}

type InsertVisitor struct {
	*BaseVisitor
}

func NewInsertVisitor() Visitor {
	return &InsertVisitor{
		BaseVisitor: &BaseVisitor{},
	}
}
func (s *InsertVisitor) Visit(tree antlr.ParseTree) any {
	ctx := tree.(*parser.RootContext)
	return  s.VisitRoot(ctx)
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
	vv, err := i.visitInsertStatementValue(insertCtx, iVal.Cols)
	if err != nil {
		return BaseVal{
			Err: err,
		}
	}
	iVal.Vals = vv
	return BaseVal{
		Data: iVal,
	}
}

func (i *InsertVisitor) visitInsertStatementValue(ctx *parser.InsertStatementValueContext, cols []string) ([]ValMap, error) {
	ans := make([]ValMap, 0, 32)
	exPressCtxs := ctx.AllExpressionsWithDefaults()
	for _, expressCtx := range exPressCtxs {
		v, err := i.visitExpressionsWithDefaults(expressCtx.(*parser.ExpressionsWithDefaultsContext), cols)
		if err != nil {
			return nil, err
		}
		ans = append(ans, v)
	}
	return ans, nil
}

func (i *InsertVisitor) visitExpressionsWithDefaults(ctx *parser.ExpressionsWithDefaultsContext, cols []string) (ValMap, error) {
	res := ValMap{}
	ivals := ctx.AllExpressionOrDefault()
	if len(cols) != len(ivals) {
		return nil, errQueryInvalid
	}
	for idx, ival := range ivals {
		v := i.VisitExpressionOrDefault(ival.(*parser.ExpressionOrDefaultContext))
		res[cols[idx]] = v.(*ValMeta)
	}
	return res, nil
}

func (i *InsertVisitor) VisitExpressionOrDefault(ctx *parser.ExpressionOrDefaultContext) any {
	return i.VisitPredicateExpression(ctx.Expression().(*parser.PredicateExpressionContext))
}

func (i *InsertVisitor) VisitPredicateExpression(ctx *parser.PredicateExpressionContext) any {
	pressCtx := ctx.GetChildren()[0].(*parser.ExpressionAtomPredicateContext)
	return i.VisitExpressionAtomPredicate(pressCtx)
}

func (i *InsertVisitor) VisitExpressionAtomPredicate(ctx *parser.ExpressionAtomPredicateContext) any {
	constantCtx := ctx.ExpressionAtom().(*parser.ConstantExpressionAtomContext)
	return i.VisitConstantExpressionAtom(constantCtx)
}

func (i *InsertVisitor) VisitConstantExpressionAtom(ctx *parser.ConstantExpressionAtomContext) any {
	constantCtx := ctx.Constant().(*parser.ConstantContext)
	return i.BaseVisitor.VisitConstant(constantCtx)
}

func (i *InsertVisitor) columns(insertStmt parser.IInsertStatementContext) []string {
	columnStmts := insertStmt.FullColumnNameList().AllFullColumnName()
	cols := make([]string, 0, len(columnStmts))
	for _, colStmt := range columnStmts {
		cols = append(cols, strings.Trim(colStmt.Uid().GetText(), "`"))
	}
	return cols
}
