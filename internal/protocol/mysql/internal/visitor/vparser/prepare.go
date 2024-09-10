package vparser

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
)

type PrepareVal struct {
	PlaceHolderCount int
}

type PrepareVisitor struct {
	*BaseVisitor
}

func NewPrepareVisitor() SqlParser {
	return &PrepareVisitor{
		BaseVisitor: &BaseVisitor{},
	}
}

func (p *PrepareVisitor) Name() string {
	return "PrepareVisitor"
}

func (p *PrepareVisitor) Parse(ctx antlr.ParseTree) any {
	ns := antlr.TreesfindAllNodes(ctx, parser.MySqlParserPLACEHOLDER, true)
	return BaseVal{
		Data: PrepareVal{
			PlaceHolderCount: len(ns),
		},
	}
}
