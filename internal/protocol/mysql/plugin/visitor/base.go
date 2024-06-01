package visitor

import (
	"errors"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
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
	return strings.Trim( val,"`")

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

func (b *BaseVisitor)VisitFullColumnNameExpressionAtom(ctx *parser.FullColumnNameExpressionAtomContext) any {
	return strings.Trim(ctx.FullColumnName().GetText(), "`")
}

func (b *BaseVisitor) removeQuote(str string)string {
	return strings.Trim(str,"`")
}
func (b *BaseVisitor)hasQuote(str string )bool {
	return strings.HasPrefix(str,"`" )
}