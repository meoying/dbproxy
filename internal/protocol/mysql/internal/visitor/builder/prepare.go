package builder

import (
	"strconv"

	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
)

type Prepare struct {
	*Base
	prepareParsedQuery pcontext.ParsedQuery
	args               []any
}

func NewPrepare(db, tab string, pctx pcontext.ParsedQuery) *Prepare {
	return &Prepare{
		Base: &Base{
			db:  db,
			tab: tab,
		},
		prepareParsedQuery: pctx,
	}
}

func (p *Prepare) Build(ctx antlr.ParseTree) (string, error) {
	return p.removeEof(ctx.GetText()), nil
}

func (p *Prepare) ReplacePlaceholders(ctx antlr.ParseTree, args []any) (string, error) {
	ns := antlr.TreesfindAllNodes(ctx, parser.MySqlParserPLACEHOLDER, true)

	for i, n := range ns {
		switch v := args[i].(type) {
		case int:
			n.(antlr.TerminalNode).GetSymbol().SetText(strconv.Itoa(v))
		case string:
			n.(antlr.TerminalNode).GetSymbol().SetText(v)
		}
	}
	return p.Build(ctx)
}

func (p *Prepare) BuildPrepareQuery(ctx antlr.ParseTree) (string, []any, error) {
	p.compareAllNodeToPlaceholder(p.prepareParsedQuery.Root(), ctx)
	q, err := p.Build(ctx)
	if err != nil {
		return "", nil, err
	}
	return q, p.args, nil
}

func (p *Prepare) compareAllNodeToPlaceholder(pctx antlr.ParseTree, ctx antlr.ParseTree) {
	// check this node (the root) first
	_, ok := ctx.(*parser.TableNameContext)
	if ok {
		return
	}

	t, ok := ctx.(*parser.ConstantContext)
	pt, ok1 := pctx.(*parser.ConstantContext)

	if ok && ok1 {
		plh := pt.PLACEHOLDER()
		if plh != nil {
			constant := t.GetChildren()[0]
			switch v := constant.(type) {
			// 字符串类型
			case *parser.StringLiteralContext:
				p.args = append(p.args, p.BaseVisitor.VisitStringLiteral(v))
			// bool
			case *parser.BooleanLiteralContext:
				p.args = append(p.args, p.BaseVisitor.VisitBooleanLiteral(v))
			case *parser.DecimalLiteralContext:
				p.args = append(p.args, p.BaseVisitor.VisitDecimalLiteral(v))
			}
			t.RemoveLastChild()
			token := t.GetStop()
			plToken := antlr.NewCommonToken(token.GetSource(), parser.MySqlParserPLACEHOLDER, token.GetChannel(), token.GetStart(), token.GetStop())
			plToken.SetText("?")
			t.AddTokenNode(plToken)
		}
	}
	// check children
	for i := 0; i < ctx.GetChildCount(); i++ {
		p.compareAllNodeToPlaceholder(pctx.GetChild(i).(antlr.ParseTree), ctx.GetChild(i).(antlr.ParseTree))
	}
}
