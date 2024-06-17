package builder

import (
	"strconv"

	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
)

const (
	SUM   = "SUM"
	COUNT = "COUNT"
)

type Select struct {
	Limit         int
	Offset        int
	ColHasChanged bool
	*Base
}

type SelectOption func(s *Select)

func WithLimit(limit, offset int) SelectOption {
	return func(s *Select) {
		s.Limit = limit
		s.Offset = offset
	}
}

func WithChanged() SelectOption {
	return func(s *Select) {
		s.ColHasChanged = true
	}
}

func NewSelect(db, tab string, opts ...SelectOption) *Select {
	s := &Select{
		Base: &Base{
			db:  db,
			tab: tab,
		},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Select) Build(ctx antlr.ParseTree) (string, error) {
	err := s.VisitRoot(ctx.(*parser.RootContext))
	if err != nil {
		return "", err.(error)
	}
	v := s.removeEof(ctx.GetText())
	return v, nil
}

func (s *Select) VisitRoot(ctx *parser.RootContext) any {
	sqlStmts := ctx.GetChildren()[0]
	sqlStmt := sqlStmts.GetChildren()[0]
	stmtctx := sqlStmt.(*parser.SqlStatementContext)
	return s.VisitSqlStatement(stmtctx)
}

func (s *Select) VisitSqlStatement(ctx *parser.SqlStatementContext) any {
	dmstmt := ctx.DmlStatement().(*parser.DmlStatementContext)
	return s.VisitDmlStatement(dmstmt)
}

func (s *Select) VisitDmlStatement(ctx *parser.DmlStatementContext) any {
	selectStmtCtx := ctx.SelectStatement().(*parser.SimpleSelectContext)
	return s.VisitSimpleSelect(selectStmtCtx)
}

func (s *Select) VisitSimpleSelect(ctx *parser.SimpleSelectContext) any {
	queryCtx := ctx.QuerySpecification()

	if !s.ColHasChanged {
		// 修改select
		s.visitSelectElements(queryCtx.SelectElements().(*parser.SelectElementsContext), 0)
		// 如果limit有值了说明需要改造limit部分
		if s.Limit != 0 {
			// 判断 有没有原ast树有没有limit子句，（判断最后一个子节点是否为limitClauseCtx)
			childLen := queryCtx.GetChildCount()
			childCtx := queryCtx.GetChild(childLen - 1)
			if _, ok := childCtx.(*parser.LimitClauseContext); ok {
				// 删除最后一个子节点（limit子句）
				queryCtx.RemoveLastChild()
			}
			// 添加limitClauseCtx节点
			limitClauseCtx := s.newLimitClause(queryCtx.(*parser.QuerySpecificationContext))
			queryCtx.AddChild(limitClauseCtx)
		}
	}
	// 改表名
	s.VisitFromClause(queryCtx.FromClause().(*parser.FromClauseContext))

	return nil
}

func (s *Select) visitSelectElements(ctx *parser.SelectElementsContext, index int) any {
	eles := ctx.AllSelectElement()
	for i := index; i < len(eles); i++ {
		ele := eles[i]
		if v, ok := ele.(*parser.SelectFunctionElementContext); ok {
			// 如果select的列为聚合函数，获取是什么类型的聚合函数
			isavg := s.VisitSelectFunctionElement(eles[i].(*parser.SelectFunctionElementContext))
			// 如果是avg
			if isavg.(bool) {
				// 将后续的列全部删除保存
				residueEles := make([]antlr.RuleContext, 0, len(eles))
				for j := i + 1; j < len(eles); j++ {
					// 删除对应的字段
					ctx.RemoveLastChild()
					// 删除逗号
					ctx.RemoveLastChild()
					residueEles = append(residueEles, eles[j])
				}
				// 在列表头部增加 sum(xxx),count(xxx)
				sumAgg := s.newSelectFunctionElementContext(v, parser.MySqlParserSUM, SUM)
				countAgg := s.newSelectFunctionElementContext(v, parser.MySqlParserCOUNT, COUNT)
				// 逗号节点
				stop := sumAgg.GetStop()
				// 位置信息是不正确的，暂时没有应用到的地方。
				commaToken := antlr.NewCommonToken(stop.GetSource(), parser.MySqlLexerCOMMA, antlr.TokenDefaultChannel, stop.GetStart()+1, stop.GetStart()+1)
				commaToken.SetText(",")
				// 重新构建select
				ctx.AddTokenNode(commaToken)
				ctx.AddChild(sumAgg)
				ctx.AddTokenNode(commaToken)
				ctx.AddChild(countAgg)
				for _, selectele := range residueEles {
					ctx.AddTokenNode(commaToken)
					ctx.AddChild(selectele)
				}
				s.visitSelectElements(ctx, index+3)
				return nil
			}
		}
	}
	return nil
}

// 构建 对照着ast解析的图来写
func (s *Select) newSelectFunctionElementContext(funcCtx *parser.SelectFunctionElementContext, parserNumber int, name string) *parser.SelectFunctionElementContext {
	funcCallCtx := funcCtx.FunctionCall().(*parser.AggregateFunctionCallContext)
	funcWinCtx := funcCallCtx.AggregateWindowedFunction().(*parser.AggregateWindowedFunctionContext)
	avgToken := funcWinCtx.AVG().GetSymbol()

	newSelectFuncCtx := parser.NewSelectFunctionElementContext(funcCtx.GetParser(), parser.NewEmptySelectElementContext())
	newSelectFuncCtx.CopyFrom(&funcCtx.BaseParserRuleContext)
	newFuncCallCtx := parser.NewAggregateFunctionCallContext(newSelectFuncCtx.GetParser(), parser.NewEmptyFunctionCallContext())
	newFuncCallCtx.CopyFrom(&(funcCallCtx.BaseParserRuleContext))
	newSelectFuncCtx.AddChild(newFuncCallCtx)

	newAggWinCtx := parser.NewAggregateWindowedFunctionContext(newFuncCallCtx.GetParser(), newFuncCallCtx, newFuncCallCtx.GetInvokingState())
	newAggWinCtx.CopyFrom(&(funcWinCtx.BaseParserRuleContext))
	newFuncCallCtx.AddChild(newAggWinCtx)
	// 什么类型的聚合函数
	aggToken := antlr.NewCommonToken(avgToken.GetSource(), parserNumber, avgToken.GetChannel(), avgToken.GetStart(), avgToken.GetStop())
	newAggWinCtx.AddTokenNode(aggToken)
	aggToken.SetText(name)

	// 左括号
	lrbracket := funcWinCtx.LR_BRACKET()
	newAggWinCtx.AddTokenNode(lrbracket.GetSymbol())
	// distinct
	if funcWinCtx.DISTINCT() != nil {
		distinctToken := funcWinCtx.DISTINCT()
		newAggWinCtx.AddTokenNode(distinctToken.GetSymbol())
	}
	// 聚合函数的字段
	funcArgCtx := funcWinCtx.FunctionArg().(*parser.FunctionArgContext)
	// 右括号
	newAggWinCtx.AddChild(funcArgCtx)
	rrbracket := funcWinCtx.RR_BRACKET()
	newAggWinCtx.AddTokenNode(rrbracket.GetSymbol())
	return newSelectFuncCtx
}

func (s *Select) VisitSelectFunctionElement(ctx *parser.SelectFunctionElementContext) any {
	switch v := ctx.FunctionCall().GetChild(0).(type) {
	case *parser.AggregateWindowedFunctionContext:
		return s.VisitAggregateWindowedFunction(v)
	default:
		return false
	}

}

func (s *Select) VisitAggregateWindowedFunction(ctx *parser.AggregateWindowedFunctionContext) any {
	if ctx.AVG() != nil {
		return true
	}
	return false
}

func (s *Select) VisitFromClause(ctx *parser.FromClauseContext) any {
	return s.VisitTableSources(ctx.TableSources().(*parser.TableSourcesContext))
}

func (s *Select) VisitTableSources(ctx *parser.TableSourcesContext) any {
	tableSourceCtx := ctx.TableSource(0)
	return s.VisitTableSourceBase(tableSourceCtx.(*parser.TableSourceBaseContext))
}

func (s *Select) VisitTableSourceBase(ctx *parser.TableSourceBaseContext) any {
	return s.VisitAtomTableItem(ctx.TableSourceItem().(*parser.AtomTableItemContext))
}

func (s *Select) VisitAtomTableItem(ctx *parser.AtomTableItemContext) any {
	return s.VisitTableName(ctx.TableName().(*parser.TableNameContext))
}

func (s *Select) newLimitClause(ctx *parser.QuerySpecificationContext) *parser.LimitClauseContext {
	newLimitClauseCtx := parser.NewLimitClauseContext(ctx.GetParser(), ctx, ctx.GetInvokingState())
	// 创建limit的token (token的start和stop我没有确定)
	token := ctx.GetStop()
	limitToken := antlr.NewCommonToken(token.GetSource(), parser.MySqlParserLIMIT, token.GetChannel(), token.GetStart(), token.GetStop())
	limitToken.SetText("LIMIT")
	newLimitClauseCtx.AddTokenNode(limitToken)
	// 创建limit的atom
	s.newLimitAtomCtx(newLimitClauseCtx, ctx, s.Limit)

	// 创建offset的token (token的start和stop我没有确定)
	offsetToken := antlr.NewCommonToken(token.GetSource(), parser.MySqlParserOFFSET, token.GetChannel(), token.GetStart(), token.GetStop())
	offsetToken.SetText("OFFSET")
	newLimitClauseCtx.AddTokenNode(offsetToken)
	// 创建offset的atom
	s.newLimitAtomCtx(newLimitClauseCtx, ctx, s.Offset)
	return newLimitClauseCtx
}

func (s *Select) newLimitAtomCtx(ctx *parser.LimitClauseContext, queryCtx *parser.QuerySpecificationContext, val int) {
	token := queryCtx.GetStop()
	// 创建  decimal_literal（token） --> decimalLiteral ---> limitClauseAtom
	decimalToken := antlr.NewCommonToken(token.GetSource(), parser.MySqlParserDECIMAL_LITERAL, token.GetChannel(), token.GetStart(), token.GetStop())
	decimalToken.SetText(strconv.Itoa(val))
	limitAtomCtx := parser.NewLimitClauseAtomContext(ctx.GetParser(), ctx, ctx.GetInvokingState())
	decimapLiteralctx := parser.NewDecimalLiteralContext(limitAtomCtx.GetParser(), limitAtomCtx, limitAtomCtx.GetInvokingState())
	decimapLiteralctx.AddTokenNode(decimalToken)
	limitAtomCtx.AddChild(decimapLiteralctx)
	ctx.AddChild(limitAtomCtx)
	return
}
