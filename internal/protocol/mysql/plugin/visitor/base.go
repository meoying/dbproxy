package visitor

import (
	"errors"
	"fmt"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/meoying/dbproxy/internal/sharding/operator"
	"reflect"
	"strconv"
	"strings"
)

var errStmtMatch = errors.New("当前语句不能使用该解析方式")
var errQueryInvalid = errors.New("当前查询错误")

type BaseVal struct {
	Err  error
	Data any
}

func getTableName(tableCtx parser.ITableNameContext) string {
	val := tableCtx.
		FullId().
		Uid(0).GetText()
	return strings.Trim(val, "`")

}

// 定义一些通用的解析方式
type BaseVisitor struct {
	parser.BaseMySqlParserVisitor
}

type ValMeta struct {
	Val any
	Typ reflect.Kind
}

func (b *BaseVisitor) VisitTableName(ctx *parser.TableNameContext) any {
	return getTableName(ctx)
}

func (b *BaseVisitor) VisitConstant(ctx *parser.ConstantContext) any {
	if ctx.GetNullLiteral() != nil {
		return &ValMeta{
			Typ: reflect.Invalid,
			Val: nil,
		}
	}
	constant := ctx.GetChildren()[0]
	switch v := constant.(type) {
	// 字符串类型
	case *parser.StringLiteralContext:
		return b.VisitStringLiteral(v)
	// bool
	case *parser.BooleanLiteralContext:
		return b.VisitBooleanLiteral(v)
	case *parser.DecimalLiteralContext:
		return b.VisitDecimalLiteral(v)
	default:
		return &ValMeta{
			Val: nil,
			Typ: reflect.Invalid,
		}
	}
}

func (b *BaseVisitor) VisitStringLiteral(ctx *parser.StringLiteralContext) any {
	return &ValMeta{
		Val: strings.Trim(ctx.GetText(), "\"'"),
		Typ: reflect.String,
	}
}

func (b *BaseVisitor) VisitBooleanLiteral(ctx *parser.BooleanLiteralContext) any {
	meta := &ValMeta{
		Typ: reflect.Bool,
	}
	if ctx.TRUE() != nil {
		meta.Val = true
	} else if ctx.FALSE() != nil {
		meta.Val = false
	}
	return meta
}

func (b *BaseVisitor) VisitDecimalLiteral(ctx *parser.DecimalLiteralContext) any {
	meta := &ValMeta{}
	if ctx.ONE_DECIMAL() != nil || ctx.TWO_DECIMAL() != nil ||
		ctx.DECIMAL_LITERAL() != nil || ctx.ZERO_DECIMAL() != nil {
		v, _ := strconv.Atoi(ctx.GetText())
		meta.Typ = reflect.Int
		meta.Val = v
	} else {
		meta.Typ = reflect.Float64
		meta.Val, _ = strconv.ParseFloat(ctx.GetText(), 64)
	}
	return meta
}

func (b *BaseVisitor) VisitFullColumnNameExpressionAtom(ctx *parser.FullColumnNameExpressionAtomContext) any {
	return strings.Trim(ctx.FullColumnName().GetText(), "`")
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
		return Predicate{}
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
	return Predicate{
		Left:  left,
		Op:    op.(operator.Op),
		Right: right,
	}
}

func (b *BaseVisitor) visitExpressionAtom(atomCtx parser.IPredicateContext) Expr {
	switch v := atomCtx.GetChild(0).(type) {
	case *parser.FullColumnNameExpressionAtomContext:
		return Column{
			Name: b.VisitFullColumnNameExpressionAtom(v).(string),
		}
	case *parser.ConstantExpressionAtomContext:
		val := b.VisitConstant(v.Constant().(*parser.ConstantContext))
		data := val.(*ValMeta).Val
		if va, ok := data.(string); ok {
			if b.hasQuote(va) {
				return Column{
					Name: b.removeQuote(va),
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
		left := b.visitMathExpression(v.GetLeft())
		right := b.visitMathExpression(v.GetRight())
		return Predicate{
			Left:  left,
			Op:    op,
			Right: right,
		}

	}
	return Column{}
}

func (b *BaseVisitor) visitMathExpression(ctx parser.IExpressionAtomContext) Expr {
	switch v := ctx.(type) {
	case *parser.FullColumnNameExpressionAtomContext:
		return Column{
			Name: v.FullColumnName().GetText(),
		}
	case *parser.ConstantExpressionAtomContext:
		val := b.VisitConstant(v.Constant().(*parser.ConstantContext))
		return ValueOf(val.(*ValMeta).Val)
	case *parser.NestedExpressionAtomContext:
		return b.VisitNestedExpressionAtom(v).(Expr)
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
		return Predicate{
			Left:  left,
			Op:    op,
			Right: right,
		}
	}
	return Column{}

}

func (b *BaseVisitor) VisitLikePredicate(ctx *parser.LikePredicateContext) any {
	op := operator.OpLike
	if ctx.NOT() != nil {
		op = operator.OpNotLike
	}
	return Predicate{
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
func (b *BaseVisitor) visitIn(ctx *parser.InPredicateContext) Predicate {
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
		ans = append(ans, b.VisitPredicateExpression(expr.(*parser.PredicateExpressionContext)).(ValueExpr).Val)
	}
	return Predicate{
		Op:   op,
		Left: col,
		Right: Values{
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
	return Predicate{
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

func (b *BaseVisitor) visitExpression(ctx parser.IExpressionContext) Expr {
	var e Expr
	switch v := ctx.(type) {
	case *parser.PredicateExpressionContext:
		return b.VisitPredicateExpression(v).(Expr)
	}

	return e

}

func (b *BaseVisitor) VisitNotExpression(ctx *parser.NotExpressionContext) any {
	pctx := ctx.Expression().(*parser.PredicateExpressionContext)
	expr := b.VisitPredicateExpression(pctx).(Expr)
	return Predicate{
		Left:  Raw(""),
		Op:    operator.OpNot,
		Right: expr,
	}
}

func (b *BaseVisitor) removeQuote(str string) string {
	return strings.Trim(str, "`")
}

func (b *BaseVisitor) hasQuote(str string) bool {
	return strings.HasPrefix(str, "`")
}

func (b *BaseVisitor) VisitComparisonOperator(ctx *parser.ComparisonOperatorContext) any {
	opstr := ctx.GetText()
	return operator.Op{
		Symbol: opstr,
		Text:   opstr,
	}
}
