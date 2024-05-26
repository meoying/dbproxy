package forward

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

var _ plugin.Plugin = &Plugin{}

type Plugin struct {
	hdl *Handler
}

func (p *Plugin) Name() string {
	return "forward"
}

func (p *Plugin) Init(cfg []byte) error {
	return nil
}

func (p *Plugin) Join(next plugin.Handler) plugin.Handler {
	return p.hdl
}

func NewPlugin(hdl *Handler) *Plugin {
	return &Plugin{
		hdl: hdl,
	}
}
