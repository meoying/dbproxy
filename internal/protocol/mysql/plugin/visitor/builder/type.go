package builder

import "github.com/antlr4-go/antlr/v4"

// SqlBuilder 用于构建sql
type SqlBuilder interface {
	// Build 输出sql
	Build(ctx antlr.ParseTree) (string, error)
}

// todo

