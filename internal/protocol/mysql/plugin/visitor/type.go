package visitor

import "github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"

type Visitor interface {
	parser.MySqlParserVisitor
	Name()string
}