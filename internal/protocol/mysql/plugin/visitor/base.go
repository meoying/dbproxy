package visitor

import (
	"strconv"
	"strings"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
)

type BaseVisitor struct {
	parser.BaseMySqlParserVisitor
}

func getTableName(tableCtx parser.ITableNameContext) string {
	val := tableCtx.
		FullId().
		Uid(0).GetText()
	return strings.Trim(val, "`")

}

func (b *BaseVisitor) VisitTableName(ctx *parser.TableNameContext) any {
	return getTableName(ctx)
}

func (b *BaseVisitor) VisitConstant(ctx *parser.ConstantContext) any {
	if ctx.GetNullLiteral() != nil {
		return nil
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
		return nil
	}
}

func (b *BaseVisitor) VisitStringLiteral(ctx *parser.StringLiteralContext) any {
	v := strings.Trim(ctx.GetText(), "'")
	return strings.Trim(v, "\"")
}

func (b *BaseVisitor) VisitBooleanLiteral(ctx *parser.BooleanLiteralContext) any {
	if ctx.TRUE() != nil {
		return true
	}
	return false
}

func (b *BaseVisitor) VisitDecimalLiteral(ctx *parser.DecimalLiteralContext) any {

	if ctx.ONE_DECIMAL() != nil || ctx.TWO_DECIMAL() != nil ||
		ctx.DECIMAL_LITERAL() != nil || ctx.ZERO_DECIMAL() != nil {
		v, _ := strconv.Atoi(ctx.GetText())
		return v
	}
	v, _ := strconv.ParseFloat(ctx.GetText(), 64)
	return v
}

func (b *BaseVisitor) VisitFullColumnNameExpressionAtom(ctx *parser.FullColumnNameExpressionAtomContext) any {
	return strings.Trim(ctx.FullColumnName().GetText(), "`")
}
func (b *BaseVisitor) VisitFullColumnName(ctx *parser.FullColumnNameContext) any {
	return b.RemoveQuote(ctx.GetText())
}

func (b *BaseVisitor) RemoveQuote(str string) string {
	return strings.Trim(str, "`")
}

func (b *BaseVisitor) HasQuote(str string) bool {
	return strings.HasPrefix(str, "`")
}
