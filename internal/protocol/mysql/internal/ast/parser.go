package ast

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
)

func Parse(query string) ProxyCtx {
	lexer := parser.NewMySqlLexer(antlr.NewInputStream(query))
	tokens := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	paser := parser.NewMySqlParser(tokens)
	root := paser.Root()
	hints := NewHintVisitor().Visit(root).(Hints)
	return ProxyCtx{
		Hints: hints,
		Root:  root,
	}
}

type ProxyCtx struct {
	Root  parser.IRootContext
	Hints Hints
}
