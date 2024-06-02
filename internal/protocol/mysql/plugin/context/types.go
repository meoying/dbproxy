package pcontext

import (
	"context"
)

type Context struct {
	context.Context
	ParsedQuery ParsedQuery
	Query       string
	Args        []any
}
