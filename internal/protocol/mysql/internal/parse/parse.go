package parse

import (
	"fmt"
	"github.com/antlr4-go/antlr/v4"
	parser "github.com/meoying/dbproxy/internal/protocol/mysql/internal/parse/parse"
)

type DemoVisitor struct {
	*parser.BaseMySqlParserVisitor
}

func Parse(sql string) {
	var is *antlr.InputStream
	// 创建输入流
	is = antlr.NewInputStream(sql)
	// 创建词法分析器
	lexer := parser.NewMySqlLexer(is)
	for {
		t := lexer.NextToken()
		if t.GetTokenType() == antlr.TokenEOF {
			break
		}
		fmt.Printf("%s (%q)\n",
			lexer.SymbolicNames[t.GetTokenType()], t.GetText())
	}
}
