package log

import (
	"log/slog"
	"os"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

type Plugin struct {
	log *slog.Logger
}

func (p *Plugin) Name() string {
	return "log"
}

func (p *Plugin) Init(cfg []byte) error {
	// TODO: 设计log插件的config.yaml, 设置slog输出的位置
	p.log = slog.New(slog.NewTextHandler(os.Stdout, nil))
	return nil
}

func (p *Plugin) Join(next plugin.Handler) plugin.Handler {
	return plugin.HandleFunc(func(ctx *pcontext.Context) (*plugin.Result, error) {
		p.log.Info("处理SQL语句：", "SQL", ctx.Query)
		return next.Handle(ctx)
	})
}
