package cmd

import "gitee.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"

type Executor interface {
	// Exec 执行命令，并且返回响应
	// 传入的 payload 部分不包含 packet 的头部字段
	// 注意，这个响应必须是合法的 mysql 协议的响应
	// 也就是说，头四个字节你必须留出来，因为在回写响应的时候要利用这四个字节设置头部
	Exec(ctx *Context, payload []byte) ([][]byte, error)
}

type Context struct {
	// CapabilityFlags 客户端支持的功能特性
	CapabilityFlags flags.CapabilityFlags
}
