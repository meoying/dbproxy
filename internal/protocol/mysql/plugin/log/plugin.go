package log

import (
	"log/slog"

	pcontext "github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

type Plugin struct {
}

func (p *Plugin) Name() string {
	// TODO implement me
	panic("implement me")
}

func (p *Plugin) Init(cfg []byte) error {
	// TODO implement me
	panic("implement me")
}

func (p *Plugin) Join(next plugin.Handler) plugin.Handler {
	return plugin.HandleFunc(func(ctx *pcontext.Context) (*plugin.Result, error) {
		slog.Debug("处理查询：", slog.String("sql", ctx.Query))
		return next.Handle(ctx)
	})
}
