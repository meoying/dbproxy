package parser

import (
	"github.com/antlr4-go/antlr/v4"
)

func (prc *FullIdContext) GetText() string {
	return getText(prc.BaseParserRuleContext)
}

func (prc *ComparisonOperatorContext) GetText() string {
	return getText(prc.BaseParserRuleContext)
}

func (prc *SelectElementsContext)GetText()string {
	return getText(prc.BaseParserRuleContext)
}



func getText(ctx antlr.BaseParserRuleContext) string {
	if ctx.GetChildCount() == 0 {
		return ""
	}
	var s string
	for _, child := range ctx.GetChildren() {
		s += child.(antlr.ParseTree).GetText()
	}
	return s
}
