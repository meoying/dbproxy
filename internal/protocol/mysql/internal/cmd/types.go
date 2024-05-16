package cmd

import (
	"context"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/connection"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
)

type Executor interface {
	// Exec 执行命令，并且返回响应
	// 传入的 payload 部分不包含 packet 的头部字段
	Exec(ctx *Context, payload []byte) error
}

type Context struct {
	context.Context
	// CapabilityFlags 客户端支持的功能特性
	CapabilityFlags flags.CapabilityFlags
	Conn            *connection.Conn
}
