package plugin

import (
	"database/sql"
	"github.com/ecodeclub/ekit/sqlx"
	pcontext "github.com/meoying/dbproxy/internal/protocol/mysql/plugin/context"
)

// Plugin 代表的是插件
// 每一个 plugin 都是独立的，并且不依赖于前后。
// plugin 要自己解决初始化、日志、加载配置等问题
// 所有的 plugin
type Plugin interface {
	// Name 名字，便于 DEBUG
	Name() string
	// Init 初始化插件
	// cfg 是你提供的配置
	Init(cfg []byte) error
	// Join 加入处理链条。你需要返回你当前处理步骤
	Join(next Handler) Handler
}

type HandleFunc func(ctx *pcontext.Context) (*Result, error)

func (h HandleFunc) Handle(ctx *pcontext.Context) (*Result, error) {
	return h(ctx)
}

type Handler interface {
	// Handle 返回的 error 只会在网关这边，而不会传递回去客户端
	Handle(ctx *pcontext.Context) (*Result, error)
}

type Result struct {
	// 这两个字段中只能有一个
	// Rows 的 error 会被传递过去客户端
	Rows sqlx.Rows
	// Result 的 error 会被传递过去客户端
	Result sql.Result
	// ChangeTransaction 是否改变事务的状态
	ChangeTransaction bool
}
