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
	// 获取到当前Query语句的底层Conn的ID
	ConnID uint32
	// 当前Query语句需要在该Stmt上执行
	StmtID uint32
}
