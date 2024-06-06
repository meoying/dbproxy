package visitor

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/meoying/dbproxy/internal/sharding/operator"
)

type UpdateVal struct {
	Assigns   []Assignable
	Predicate Predicate
}

type UpdateVisitor struct {
	*BaseVisitor
}

func NewUpdateVisitor() Visitor {
	return &UpdateVisitor{
		BaseVisitor: &BaseVisitor{},
	}
}
func (u *UpdateVisitor) Visit(tree antlr.ParseTree) any {
	ctx := tree.(*parser.RootContext)
	return u.VisitRoot(ctx)
}

func (u *UpdateVisitor) Name() string {
	return "UpdateVisitor"
}

func (u *UpdateVisitor) VisitRoot(ctx *parser.RootContext) any {
	sqlStmts := ctx.GetChildren()[0]
	sqlStmt := sqlStmts.GetChildren()[0]
	stmtctx := sqlStmt.(*parser.SqlStatementContext)
	return u.VisitSqlStatement(stmtctx)
}

func (u *UpdateVisitor) VisitSqlStatement(ctx *parser.SqlStatementContext) any {
	dmstmt := ctx.DmlStatement().(*parser.DmlStatementContext)
	return u.VisitDmlStatement(dmstmt)
}

func (u *UpdateVisitor) VisitDmlStatement(ctx *parser.DmlStatementContext) any {
	updateStmt := ctx.UpdateStatement()
	if updateStmt.SingleUpdateStatement() == nil {
		return BaseVal{
			Err: ErrUnsupportedUpdateSql,
		}
	}
	return u.VisitSingleUpdateStatement(updateStmt.SingleUpdateStatement().(*parser.SingleUpdateStatementContext))
}

func (u *UpdateVisitor) VisitSingleUpdateStatement(ctx *parser.SingleUpdateStatementContext) any {
	// where 条件
	pre := u.visitWhere(ctx.Expression())
	// set 后面的列
	updateEles := ctx.AllUpdatedElement()
	assigns := make([]Assignable,0,len(updateEles))
	for _, ele := range updateEles {
		res :=  u.VisitUpdatedElement(ele.(*parser.UpdatedElementContext))
		if err,ok :=  res.(error);ok {
			return BaseVal{
				Err: err,
			}
		}
		assigns = append(assigns,res.(Assignment))
	}
	return BaseVal{
		Data: UpdateVal{
			Predicate: pre.(Predicate),
			Assigns: assigns,
		},
	}
}

func (u *UpdateVisitor) VisitUpdatedElement(ctx *parser.UpdatedElementContext) any {
	columnName := u.BaseVisitor.VisitFullColumnName(ctx.FullColumnName().(*parser.FullColumnNameContext))
	v := u.VisitPredicateExpression(ctx.Expression().(*parser.PredicateExpressionContext))
	val,ok := v.(Expr)
	if !ok {
		return ErrUnsupportedUpdateSql
	}
	return Assignment{
		Left: Column{
			Name: columnName.(string),
		},
		Op: operator.OpEQ,
		Right: val,
	}
}
