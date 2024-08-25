package ast

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/vparser"
)

func Parse(query string) ProxyCtx {
	lexer := parser.NewMySqlLexer(antlr.NewInputStream(query))
	tokens := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	paser := parser.NewMySqlParser(tokens)
	root :=  paser.Root()
	hints := vparser.NewHintVisitor().Visit(root).(vparser.Hints)
	return ProxyCtx{
		Hints: hints,
		Root: root,
	}
}
type ProxyCtx struct {
	Root parser.IRootContext
	Hints vparser.Hints
}