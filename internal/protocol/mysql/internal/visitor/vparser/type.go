package vparser

import "github.com/antlr4-go/antlr/v4"

// SqlParser 用于解析sql
type SqlParser interface {
	Parse(ctx antlr.ParseTree) any
}
