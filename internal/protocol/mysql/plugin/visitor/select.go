package visitor

import (
	"fmt"
	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/meoying/dbproxy/internal/sharding/operator"
	"strings"
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
	whereExpr := ctx.Expression()
	switch v := whereExpr.(type) {
	case *parser.PredicateExpressionContext:
		return s.VisitPredicateExpression(v)
	case *parser.LogicalExpressionContext:
		return s.VisitLogicalExpression(v)
	case *parser.NotExpressionContext:
		return s.VisitNotExpression(v)
	}
	return nil
}

func (s *SelectVisitor) VisitNotExpression(ctx *parser.NotExpressionContext) any {
	pctx := ctx.Expression().(*parser.PredicateExpressionContext)
	expr := s.VisitPredicateExpression(pctx).(Expr)
	return Predicate{
		Left:  Raw(""),
		Op:    operator.OpNot,
		Right: expr,
	}
}

func (s *SelectVisitor) VisitLogicalExpression(ctx *parser.LogicalExpressionContext) any {
	opVal := strings.ToUpper(ctx.LogicalOperator().GetText())
	op := operator.Op{
		Symbol: opVal,
		Text:   fmt.Sprintf(" %s ", opVal),
	}
	left := s.visitExpression(ctx.Expression(0))
	right := s.visitExpression(ctx.Expression(1))
	return Predicate{
		Left:  left,
		Op:    op,
		Right: right,
	}

}

func (s *SelectVisitor) visitExpression(ctx parser.IExpressionContext) Expr {
	var e Expr
	switch v := ctx.(type) {
	case *parser.PredicateExpressionContext:
		return s.VisitPredicateExpression(v).(Expr)
	}

	return e

}

func (s *SelectVisitor) VisitPredicateExpression(ctx *parser.PredicateExpressionContext) any {

	switch v := ctx.Predicate().(type) {
	case *parser.BinaryComparisonPredicateContext:
		return s.VisitBinaryComparisonPredicate(v)
	case *parser.LikePredicateContext:
		return s.VisitLikePredicate(v)
	case *parser.InPredicateContext:
		return s.VisitInPredicate(v)
	case *parser.ExpressionAtomPredicateContext:
		return s.VisitExpressionAtomPredicate(v)
	}
	return nil
}

func (s *SelectVisitor) VisitExpressionAtomPredicate(ctx *parser.ExpressionAtomPredicateContext) any {
	return s.visitMathExpression(ctx.ExpressionAtom())

}

func (s *SelectVisitor) VisitNestedExpressionAtom(ctx *parser.NestedExpressionAtomContext) any {
	switch v := ctx.Expression(0).(type) {
	case *parser.LogicalExpressionContext:
		return s.VisitLogicalExpression(v)
	case *parser.PredicateExpressionContext:
		return s.VisitPredicateExpression(v)

	}
	return nil
}

func (s *SelectVisitor) VisitInPredicate(ctx *parser.InPredicateContext) any {
	if ctx.IN() != nil {
		return s.visitIn(ctx)
	}

	return nil
}

// visitIn 不处理子查询
func (s *SelectVisitor) visitIn(ctx *parser.InPredicateContext) Predicate {
	var op operator.Op
	if ctx.IN() != nil  {
		op = operator.OpIn
		if ctx.NOT() != nil {
			op = operator.OpNotIN
		}
	}
	col := s.visitExpressionAtom(ctx.Predicate())
	exprs := ctx.Expressions().AllExpression()
	ans := make([]any, 0, len(exprs))
	for _, expr := range exprs {
		ans = append(ans, s.VisitPredicateExpression(expr.(*parser.PredicateExpressionContext)).(ValueExpr).Val)
	}
	return Predicate{
		Op:   op,
		Left: col,
		Right: Values{
			Vals: ans,
		},
	}
}

func (s *SelectVisitor) VisitLikePredicate(ctx *parser.LikePredicateContext) any {
	op := operator.OpLike
	if ctx.NOT() != nil {
		op = operator.OpNotLike
	}
	return Predicate{
		Left:  s.visitExpressionAtom(ctx.Predicate(0)),
		Op:    op,
		Right: s.visitExpressionAtom(ctx.Predicate(1)),
	}

}

func (s *SelectVisitor) VisitBinaryComparisonPredicate(ctx *parser.BinaryComparisonPredicateContext) any {
	left := s.visitExpressionAtom(ctx.GetLeft())
	op := s.VisitComparisonOperator(ctx.ComparisonOperator().(*parser.ComparisonOperatorContext))
	right := s.visitExpressionAtom(ctx.GetRight())
	return Predicate{
		Left:  left,
		Op:    op.(operator.Op),
		Right: right,
	}
}

func (s *SelectVisitor) visitExpressionAtom(atomCtx parser.IPredicateContext) Expr {
	switch v := atomCtx.GetChild(0).(type) {
	case *parser.FullColumnNameExpressionAtomContext:
		return Column{
			Name: s.BaseVisitor.VisitFullColumnNameExpressionAtom(v).(string),
		}
	case *parser.ConstantExpressionAtomContext:
		val := s.BaseVisitor.VisitConstant(v.Constant().(*parser.ConstantContext))
		data := val.(*ValMeta).Val
		if va,ok := data.(string);ok{
			if s.BaseVisitor.hasQuote(va) {
				return Column{
					Name: s.BaseVisitor.removeQuote(va),
				}
			}
		}
		return ValueOf(data)
	case *parser.MathExpressionAtomContext:
		var op operator.Op
		if v.MultOperator() != nil {
			op.Text = v.MultOperator().GetText()
			op.Symbol = v.MultOperator().GetText()
		} else if v.AddOperator() != nil {
			op.Text = v.AddOperator().GetText()
			op.Symbol = v.AddOperator().GetText()
		}
		left := s.visitMathExpression(v.GetLeft())
		right := s.visitMathExpression(v.GetRight())
		return Predicate{
			Left:  left,
			Op:    op,
			Right: right,
		}

	}
	return Column{}
}

func (s *SelectVisitor) visitMathExpression(ctx parser.IExpressionAtomContext) Expr {
	switch v := ctx.(type) {
	case *parser.FullColumnNameExpressionAtomContext:
		return Column{
			Name: v.FullColumnName().GetText(),
		}
	case *parser.ConstantExpressionAtomContext:
		val := s.BaseVisitor.VisitConstant(v.Constant().(*parser.ConstantContext))
		return ValueOf(val.(*ValMeta).Val)
	case *parser.NestedExpressionAtomContext:

		return s.VisitNestedExpressionAtom(v).(Expr)
	case *parser.MathExpressionAtomContext:
		var op operator.Op
		if v.MultOperator() != nil {
			op.Text = v.MultOperator().GetText()
			op.Symbol = v.MultOperator().GetText()
		} else if v.AddOperator() != nil {
			op.Text = v.AddOperator().GetText()
			op.Symbol = v.AddOperator().GetText()
		}
		left := s.visitMathExpression(v.GetLeft())
		right := s.visitMathExpression(v.GetRight())
		return Predicate{
			Left:  left,
			Op:    op,
			Right: right,
		}
	}
	return Column{}

}

func (s *SelectVisitor) VisitComparisonOperator(ctx *parser.ComparisonOperatorContext) any {
	opstr := ctx.GetText()
	return operator.Op{
		Symbol: opstr,
		Text:   opstr,
	}
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
