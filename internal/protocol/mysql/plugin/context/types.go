package pcontext

import (
	"context"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor"
)

type Context struct {
	context.Context
	// 这个是解析后的
	Visitors    map[string]visitor.Visitor
	ParsedQuery ParsedQuery
	Query       string
	Args        []any
}
