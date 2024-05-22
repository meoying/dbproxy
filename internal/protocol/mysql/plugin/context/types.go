package pcontext

import (
	"context"
)

type Context struct {
	context.Context
	// 这个是解析后的
	ParsedQuery ParsedQuery
	Query       string
	Args        []any
}
