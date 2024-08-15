package ast

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
)

func Parse(query string) parser.IRootContext {
	lexer := parser.NewMySqlLexer(antlr.NewInputStream(query))
	tokens := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	paser := parser.NewMySqlParser(tokens)
	return paser.Root()
}
