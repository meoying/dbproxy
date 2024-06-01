package forward

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor"
)

var _ plugin.Plugin = &Plugin{}

type Plugin struct {
	hdl *Handler
}

func (p *Plugin) NewVisitor() map[string]visitor.Visitor {
	panic("implement me")
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
