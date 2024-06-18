package vparser

import (
	"fmt"
	"strings"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor"
	"github.com/meoying/dbproxy/internal/sharding/operator"
)

type BaseVal struct {
	Err  error
	Data any
}

// 定义一些通用的解析方式
type BaseVisitor struct {
	visitor.BaseVisitor
}

// visitWhere delete，select，update 解析where部分的语句
func (b *BaseVisitor) visitWhere(ctx parser.IExpressionContext) any {
	switch v := ctx.(type) {
	case *parser.PredicateExpressionContext:
		return b.VisitPredicateExpression(v)
	case *parser.LogicalExpressionContext:
		return b.VisitLogicalExpression(v)
	case *parser.NotExpressionContext:
		return b.VisitNotExpression(v)
	default:
		return visitor.Predicate{}
	}
}

func (b *BaseVisitor) VisitPredicateExpression(ctx *parser.PredicateExpressionContext) any {
	switch v := ctx.Predicate().(type) {
	case *parser.BinaryComparisonPredicateContext:
		return b.VisitBinaryComparisonPredicate(v)
	case *parser.LikePredicateContext:
		return b.VisitLikePredicate(v)
	case *parser.InPredicateContext:
		return b.VisitInPredicate(v)
	case *parser.ExpressionAtomPredicateContext:
		return b.VisitExpressionAtomPredicate(v)
	}
	return nil
}

func (b *BaseVisitor) VisitBinaryComparisonPredicate(ctx *parser.BinaryComparisonPredicateContext) any {
	left := b.visitExpressionAtom(ctx.GetLeft())
	op := b.VisitComparisonOperator(ctx.ComparisonOperator().(*parser.ComparisonOperatorContext))
	right := b.visitExpressionAtom(ctx.GetRight())
	return visitor.Predicate{
		Left:  left,
		Op:    op.(operator.Op),
		Right: right,
	}
}

func (b *BaseVisitor) visitExpressionAtom(atomCtx parser.IPredicateContext) visitor.Expr {
	switch v := atomCtx.GetChild(0).(type) {
	case *parser.FullColumnNameExpressionAtomContext:
		return visitor.Column{
			Name: b.VisitFullColumnNameExpressionAtom(v).(string),
		}
	case *parser.ConstantExpressionAtomContext:
		val := b.VisitConstant(v.Constant().(*parser.ConstantContext))
		if va, ok := val.(string); ok {
			if b.HasQuote(va) {
				return visitor.Column{
					Name: b.RemoveQuote(va),
				}
			}
		}
		return visitor.ValueOf(val)
	case *parser.MathExpressionAtomContext:
		var op operator.Op
		if v.MultOperator() != nil {
			op.Text = v.MultOperator().GetText()
			op.Symbol = v.MultOperator().GetText()
		} else if v.AddOperator() != nil {
			op.Text = v.AddOperator().GetText()
			op.Symbol = v.AddOperator().GetText()
		}
		left := b.visitMathExpression(v.GetLeft())
		right := b.visitMathExpression(v.GetRight())
		return visitor.Predicate{
			Left:  left,
			Op:    op,
			Right: right,
		}

	}
	return visitor.Column{}
}

func (b *BaseVisitor) visitMathExpression(ctx parser.IExpressionAtomContext) visitor.Expr {
	switch v := ctx.(type) {
	case *parser.FullColumnNameExpressionAtomContext:
		return visitor.Column{
			Name: b.RemoveQuote(v.FullColumnName().GetText()),
		}
	case *parser.ConstantExpressionAtomContext:
		val := b.VisitConstant(v.Constant().(*parser.ConstantContext))
		return visitor.ValueOf(val)
	case *parser.NestedExpressionAtomContext:
		return b.VisitNestedExpressionAtom(v).(visitor.Expr)
	case *parser.MathExpressionAtomContext:
		var op operator.Op
		if v.MultOperator() != nil {
			op.Text = v.MultOperator().GetText()
			op.Symbol = v.MultOperator().GetText()
		} else if v.AddOperator() != nil {
			op.Text = v.AddOperator().GetText()
			op.Symbol = v.AddOperator().GetText()
		}
		left := b.visitMathExpression(v.GetLeft())
		right := b.visitMathExpression(v.GetRight())
		return visitor.Predicate{
			Left:  left,
			Op:    op,
			Right: right,
		}
	}
	return visitor.Column{}

}

func (b *BaseVisitor) VisitLikePredicate(ctx *parser.LikePredicateContext) any {
	op := operator.OpLike
	if ctx.NOT() != nil {
		op = operator.OpNotLike
	}
	return visitor.Predicate{
		Left:  b.visitExpressionAtom(ctx.Predicate(0)),
		Op:    op,
		Right: b.visitExpressionAtom(ctx.Predicate(1)),
	}

}

func (b *BaseVisitor) VisitInPredicate(ctx *parser.InPredicateContext) any {
	if ctx.IN() != nil {
		return b.visitIn(ctx)
	}

	return nil
}

// visitIn 不处理子查询
func (b *BaseVisitor) visitIn(ctx *parser.InPredicateContext) visitor.Predicate {
	var op operator.Op
	if ctx.IN() != nil {
		op = operator.OpIn
		if ctx.NOT() != nil {
			op = operator.OpNotIN
		}
	}
	col := b.visitExpressionAtom(ctx.Predicate())
	exprs := ctx.Expressions().AllExpression()
	ans := make([]any, 0, len(exprs))
	for _, expr := range exprs {
		ans = append(ans, b.VisitPredicateExpression(expr.(*parser.PredicateExpressionContext)).(visitor.ValueExpr).Val)
	}
	return visitor.Predicate{
		Op:   op,
		Left: col,
		Right: visitor.Values{
			Vals: ans,
		},
	}
}

func (b *BaseVisitor) VisitExpressionAtomPredicate(ctx *parser.ExpressionAtomPredicateContext) any {
	return b.visitMathExpression(ctx.ExpressionAtom())

}

func (b *BaseVisitor) VisitLogicalExpression(ctx *parser.LogicalExpressionContext) any {
	opVal := strings.ToUpper(ctx.LogicalOperator().GetText())
	op := operator.Op{
		Symbol: opVal,
		Text:   fmt.Sprintf(" %s ", opVal),
	}
	left := b.visitExpression(ctx.Expression(0))
	right := b.visitExpression(ctx.Expression(1))
	return visitor.Predicate{
		Left:  left,
		Op:    op,
		Right: right,
	}
}

func (b *BaseVisitor) VisitNestedExpressionAtom(ctx *parser.NestedExpressionAtomContext) any {
	switch v := ctx.Expression(0).(type) {
	case *parser.LogicalExpressionContext:
		return b.VisitLogicalExpression(v)
	case *parser.PredicateExpressionContext:
		return b.VisitPredicateExpression(v)

	}
	return nil
}

func (b *BaseVisitor) visitExpression(ctx parser.IExpressionContext) visitor.Expr {
	var e visitor.Expr
	switch v := ctx.(type) {
	case *parser.PredicateExpressionContext:
		return b.VisitPredicateExpression(v).(visitor.Expr)
	}
	return e
}

func (b *BaseVisitor) VisitNotExpression(ctx *parser.NotExpressionContext) any {
	pctx := ctx.Expression().(*parser.PredicateExpressionContext)
	expr := b.VisitPredicateExpression(pctx).(visitor.Expr)
	return visitor.Predicate{
		Left:  visitor.Raw(""),
		Op:    operator.OpNot,
		Right: expr,
	}
}

func (b *BaseVisitor) VisitComparisonOperator(ctx *parser.ComparisonOperatorContext) any {
	opstr := ctx.GetText()
	return operator.Op{
		Symbol: opstr,
		Text:   opstr,
	}
}
