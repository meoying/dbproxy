package vparser

import (
	"strconv"

	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor"
)

type SelectVal struct {
	Cols          []visitor.Selectable
	Predicate     visitor.Predicate
	Distinct      bool
	OrderClauses  []OrderClause
	LimitClause   *LimitClause
	GroupByClause []string
}

type OrderClause struct {
	Column string
	Order  string
}

type LimitClause struct {
	Limit  int
	Offset int
}

type SelectVisitor struct {
	*BaseVisitor
}

func (s *SelectVisitor) Name() string {
	return "SelectVisitor"
}

func NewsSelectVisitor() SqlParser {
	return &SelectVisitor{
		BaseVisitor: &BaseVisitor{},
	}
}

func (s *SelectVisitor) Parse(ctx antlr.ParseTree) any {
	return s.Visit(ctx)
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
	selectVal := SelectVal{}
	// 是否含有distinct
	if len(queryCtx.AllSelectSpec()) > 0 {
		if queryCtx.SelectSpec(0).DISTINCT() != nil {
			selectVal.Distinct = true
		}
	}
	// 处理select部分
	selectVal.Cols = s.VisitSelectElements(queryCtx.SelectElements().(*parser.SelectElementsContext)).([]visitor.Selectable)
	// 处理where和from部分
	selectVal.Predicate = s.VisitFromClause(queryCtx.FromClause().(*parser.FromClauseContext)).(visitor.Predicate)
	// 处理group by 部分,这部分不是sql语句中必须有的部分，要先判断是否存在
	if queryCtx.GroupByClause() != nil {
		groupByClauses := s.VisitGroupByClause(queryCtx.GroupByClause().(*parser.GroupByClauseContext))
		if err, ok := groupByClauses.(error); ok {
			return BaseVal{
				Err: err,
			}
		}
		selectVal.GroupByClause = groupByClauses.([]string)
	}
	// 处理order by 部分
	if queryCtx.OrderByClause() != nil {
		orderByClauses := s.VisitOrderByClause(queryCtx.OrderByClause().(*parser.OrderByClauseContext))
		if err, ok := orderByClauses.(error); ok {
			return BaseVal{
				Err: err,
			}
		}
		selectVal.OrderClauses = orderByClauses.([]OrderClause)
	}
	// 处理limit 部分
	if queryCtx.LimitClause() != nil {
		limitClause := s.VisitLimitClause(queryCtx.LimitClause().(*parser.LimitClauseContext))
		selectVal.LimitClause = limitClause.(*LimitClause)
	}
	return BaseVal{
		Data: selectVal,
	}
}

// VisitFromClause 处理where部分
func (s *SelectVisitor) VisitFromClause(ctx *parser.FromClauseContext) any {
	if ctx.WHERE() == nil {
		return visitor.Predicate{}
	}
	return s.visitWhere(ctx.Expression())
}

// VisitTableSources 处理表名 不处理join查询和子查询（暂时没有用到）
func (s *SelectVisitor) VisitTableSources(ctx *parser.TableSourcesContext) any {
	tableCtx := ctx.GetChild(0).(*parser.AtomTableItemContext).TableName()
	return s.BaseVisitor.VisitTableName(tableCtx.(*parser.TableNameContext))
}

// VisitSelectElements 处理 select部分
func (s *SelectVisitor) VisitSelectElements(ctx *parser.SelectElementsContext) any {
	colEles := ctx.GetChildren()
	cols := make([]visitor.Selectable, 0, len(colEles))
	if ctx.STAR() != nil {
		return cols
	}
	for _, colEle := range colEles {
		switch v := colEle.(type) {
		case *parser.SelectColumnElementContext:
			col := s.VisitSelectColumnElement(v)
			cols = append(cols, col.(visitor.Column))
		case *parser.SelectFunctionElementContext:
			col := s.VisitSelectFunctionElement(v)
			cols = append(cols, col.(visitor.Aggregate))
		}
	}
	return cols
}

// VisitSelectColumnElement 处理 select的字段部分
func (s *SelectVisitor) VisitSelectColumnElement(ctx *parser.SelectColumnElementContext) any {
	va := ctx.FullColumnName().GetText()
	col := visitor.Column{
		Name: s.BaseVisitor.RemoveQuote(va),
	}
	if ctx.AS() != nil {
		col.Alias = s.BaseVisitor.RemoveQuote(ctx.Uid().GetText())
	}
	return col
}

// VisitSelectFunctionElement 处理select的聚合函数部分
func (s *SelectVisitor) VisitSelectFunctionElement(ctx *parser.SelectFunctionElementContext) any {
	resp := s.VisitAggregateFunctionCall(ctx.FunctionCall().(*parser.AggregateFunctionCallContext))
	agg := resp.(visitor.Aggregate)
	if ctx.AS() != nil {
		alias := s.BaseVisitor.RemoveQuote(ctx.Uid().GetText())
		agg.Alias = alias
	}
	return agg
}

// VisitGroupByClause 处理group by
func (s *SelectVisitor) VisitGroupByClause(ctx *parser.GroupByClauseContext) any {
	if ctx == nil {
		return []visitor.Column{}
	}
	items := ctx.AllGroupByItem()
	groupByCols := make([]string, 0, len(items))
	for _, item := range items {
		col := s.BaseVisitor.VisitPredicateExpression(item.Expression().(*parser.PredicateExpressionContext))
		switch v := col.(type) {
		case visitor.Column:
			groupByCols = append(groupByCols, v.Name)
		case visitor.ValueExpr:
			groupByCols = append(groupByCols, s.RemoveQuote(v.Val.(string)))
		default:
			return errUnsupportedGroupByClause
		}

	}
	return groupByCols
}

// VisitOrderByClause 处理order by
func (s *SelectVisitor) VisitOrderByClause(ctx *parser.OrderByClauseContext) any {
	if ctx == nil {
		return []OrderClause{}
	}
	orderByExpresses := ctx.AllOrderByExpression()
	orderByClauses := make([]OrderClause, 0, len(orderByExpresses))
	for _, orderExpr := range orderByExpresses {
		resp := s.VisitOrderByExpression(orderExpr.(*parser.OrderByExpressionContext))
		if err, ok := resp.(error); ok {
			return err
		}
		orderByClauses = append(orderByClauses, resp.(OrderClause))
	}
	return orderByClauses
}

func (s *SelectVisitor) VisitOrderByExpression(ctx *parser.OrderByExpressionContext) any {
	orderClause := OrderClause{}
	col := s.BaseVisitor.VisitPredicateExpression(ctx.Expression().(*parser.PredicateExpressionContext))
	switch v := col.(type) {
	case visitor.Column:
		orderClause.Column = v.Name
	case visitor.ValueExpr:
		orderClause.Column = s.RemoveQuote(v.Val.(string))
	default:
		return errUnsupportedOrderByClause
	}
	if ctx.DESC() != nil {
		orderClause.Order = "DESC"
	} else {
		orderClause.Order = "ASC"
	}
	return orderClause
}

// VisitLimitClause 处理Limit
func (s *SelectVisitor) VisitLimitClause(ctx *parser.LimitClauseContext) any {
	var limitClause *LimitClause
	if ctx.LIMIT() != nil {
		limit := s.VisitLimitClauseAtom(ctx.LimitClauseAtom(0).(*parser.LimitClauseAtomContext))
		limitClause = &LimitClause{
			Limit: limit.(int),
		}
		if ctx.OFFSET() != nil {
			offset := s.VisitLimitClauseAtom(ctx.LimitClauseAtom(1).(*parser.LimitClauseAtomContext))
			limitClause.Offset = offset.(int)
		}
	}
	return limitClause
}

func (s *SelectVisitor) VisitLimitClauseAtom(ctx *parser.LimitClauseAtomContext) any {
	meta := s.BaseVisitor.VisitDecimalLiteral(ctx.DecimalLiteral().(*parser.DecimalLiteralContext))
	return meta.(int)
}

// 处理聚合函数
func (s *SelectVisitor) VisitAggregateFunctionCall(ctx *parser.AggregateFunctionCallContext) any {
	aggCtx := ctx.AggregateWindowedFunction()
	var name string
	if aggCtx.STAR() != nil {
		name = "*"
	} else if aggCtx.FunctionArg() != nil {
		if aggCtx.FunctionArg().FullColumnName() != nil {
			name = s.VisitFullColumnName(aggCtx.FunctionArg().FullColumnName().(*parser.FullColumnNameContext)).(string)
		} else if aggCtx.FunctionArg().Constant() != nil {
			val := s.VisitConstant(aggCtx.FunctionArg().Constant().(*parser.ConstantContext))
			switch constant := val.(type) {
			case string:
				name = constant
			case int:
				name = strconv.Itoa(constant)
			}
		}
	}
	var agg visitor.Aggregate
	switch {
	case aggCtx.AVG() != nil:
		agg = visitor.NewAggregate(name, aggCtx.AVG().GetText())
	case aggCtx.MIN() != nil:
		agg = visitor.NewAggregate(name, aggCtx.MIN().GetText())
	case aggCtx.MAX() != nil:
		agg = visitor.NewAggregate(name, aggCtx.MAX().GetText())
	case aggCtx.SUM() != nil:
		agg = visitor.NewAggregate(name, aggCtx.SUM().GetText())
	case aggCtx.COUNT() != nil:
		agg = visitor.NewAggregate(name, aggCtx.COUNT().GetText())
	}
	if aggCtx.DISTINCT() != nil {
		agg.Distinct = true
	}
	return agg

}
