package vparser

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
)

type HintValue struct {
	Key   string
	Value string
}
func (s *HintVisitor) Name() string {
	return "HintVisitor"
}

type HintVisitor struct {
	*BaseVisitor
}

func NewHintVisitor() *HintVisitor {
	return &HintVisitor{}
}

func (s *HintVisitor) Visit(tree antlr.ParseTree) any {
	ctx := tree.(*parser.RootContext)
	return s.VisitRoot(ctx)
}

func (s *HintVisitor) VisitRoot(ctx *parser.RootContext) any {
	sqlStmts := ctx.GetChildren()[0]
	sqlStmt := sqlStmts.GetChildren()[0]
	return s.VisitSqlStatement(sqlStmt.(*parser.SqlStatementContext))
}

func (s *HintVisitor) VisitSqlStatement(ctx *parser.SqlStatementContext) any {
	 switch v := ctx.GetChild(0).(type) {
	 case *parser.DmlStatementContext:
		return  s.VisitDmlStatement(v)
	 case *parser.TransactionStatementContext:
		return  s.VisitTransactionStatement(v)
	 }
	 return map[string]HintValue{}
}

func (s *HintVisitor) VisitTransactionStatement(ctx *parser.TransactionStatementContext) any {
	switch v := ctx.GetChild(0).(type) {
	case *parser.BeginWorkContext:
		return s.VisitBeginWork(v)
	case *parser.CommitWorkContext:
		return  s.VisitCommitWork(v)
	case *parser.RollbackWorkContext:
		return s.VisitRollbackWork(v)
	}
	return map[string]HintValue{}
}


func (s *HintVisitor) VisitBeginWork(ctx *parser.BeginWorkContext) any{
	if ctx.ProxyHint() != nil {
		return s.VisitProxyHint(ctx.ProxyHint().(*parser.ProxyHintContext))
	}
	return map[string]HintValue{}
}

func (s *HintVisitor) VisitCommitWork(ctx *parser.CommitWorkContext) any {
	if ctx.ProxyHint() != nil {
		return s.VisitProxyHint(ctx.ProxyHint().(*parser.ProxyHintContext))
	}
	return map[string]HintValue{}
}

func (s *HintVisitor) VisitRollbackWork(ctx *parser.RollbackWorkContext) interface{} {
	if ctx.ProxyHint() != nil {
		return s.VisitProxyHint(ctx.ProxyHint().(*parser.ProxyHintContext))
	}
	return map[string]HintValue{}
}

func (s *HintVisitor) VisitDmlStatement(ctx *parser.DmlStatementContext) any {
	switch v := ctx.GetChild(0).(type) {
	case *parser.SimpleSelectContext:
		return s.VisitSimpleSelect(v)
	case *parser.UpdateStatementContext:
		return s.VisitUpdateStatement(v)
	case *parser.InsertStatementContext:
		return s.VisitInsertStatement(v)
	case *parser.DeleteStatementContext:
		return s.VisitDeleteStatement(v)
	}
	return map[string]HintValue{}
}

func (s *HintVisitor) VisitDeleteStatement(ctx *parser.DeleteStatementContext) any {
	if ctx.SingleDeleteStatement() != nil {
		return s.VisitSingleDeleteStatement(ctx.SingleDeleteStatement().(*parser.SingleDeleteStatementContext))
	}
	if ctx.MultipleDeleteStatement() != nil {
		return s.VisitMultipleDeleteStatement(ctx.MultipleDeleteStatement().(*parser.MultipleDeleteStatementContext))
	}
	return map[string]HintValue{}

}

func (s *HintVisitor) VisitSingleDeleteStatement(ctx *parser.SingleDeleteStatementContext) interface{} {
	if ctx.ProxyHint() != nil {
		return s.VisitProxyHint(ctx.ProxyHint().(*parser.ProxyHintContext))
	}
	return map[string]HintValue{}
}

func (s *HintVisitor) VisitMultipleDeleteStatement(ctx *parser.MultipleDeleteStatementContext) interface{} {
	if ctx.ProxyHint() != nil {
		return s.VisitProxyHint(ctx.ProxyHint().(*parser.ProxyHintContext))
	}
	return map[string]HintValue{}
}

func (s *HintVisitor) VisitUpdateStatement(ctx *parser.UpdateStatementContext) any {
	if ctx.SingleUpdateStatement() != nil {
		return s.VisitSingleUpdateStatement(ctx.SingleUpdateStatement().(*parser.SingleUpdateStatementContext))
	}
	if ctx.MultipleUpdateStatement() != nil {
		return s.VisitMultipleUpdateStatement(ctx.MultipleUpdateStatement().(*parser.MultipleUpdateStatementContext))
	}
	return map[string]HintValue{}
}

func (s *HintVisitor) VisitSingleUpdateStatement(ctx *parser.SingleUpdateStatementContext) any {
	if ctx.ProxyHint() != nil {
		return s.VisitProxyHint(ctx.ProxyHint().(*parser.ProxyHintContext))
	}
	return map[string]HintValue{}
}

func (s *HintVisitor) VisitSimpleSelect(ctx *parser.SimpleSelectContext) any {
	queryCtx := ctx.QuerySpecification()
	if queryCtx.ProxyHint() != nil {
		return s.VisitProxyHint(queryCtx.ProxyHint().(*parser.ProxyHintContext))
	}
	return map[string]HintValue{}
}

func (s *HintVisitor) VisitProxyHint(ctx *parser.ProxyHintContext) any {
	children := ctx.GetChildren()
	hintMaps := make(map[string]HintValue, len(children))
	for _, child := range children {
		annotation, ok := child.(*parser.AnnotationContext)
		if ok {
			hintVal := s.VisitAnnotation(annotation).(HintValue)
			hintMaps[hintVal.Key] = hintVal
		}
	}
	return hintMaps
}

func (s *HintVisitor) VisitAnnotation(ctx *parser.AnnotationContext) any {
	key := ctx.Key().GetText()
	val := ctx.Value().GetText()
	return HintValue{Key: key, Value: val}
}

func (s *HintVisitor) VisitInsertStatement(ctx *parser.InsertStatementContext) any {
	if ctx.ProxyHint() != nil {
		return s.VisitProxyHint(ctx.ProxyHint().(*parser.ProxyHintContext))
	}
	return map[string]HintValue{}
}
