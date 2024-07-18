package sharding

import (
	"encoding/json"

	shardingconfig "github.com/meoying/dbproxy/config/mysql/sharding"
	"github.com/meoying/dbproxy/internal/protocol/mysql/configbuilder"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

type Plugin struct {
	hdl *Handler
}

func (p *Plugin) Name() string {
	return "sharding"
}

func (p *Plugin) Init(cfg []byte) error {
	var config shardingconfig.Config
	err := json.Unmarshal(cfg, &config)
	if err != nil {
		return err
	}
	var cfgBuilder configbuilder.ShardingConfigBuilder
	cfgBuilder.SetConfig(config)

	algorithm, err := cfgBuilder.BuildAlgorithm()
	if err != nil {
		return err
	}
	ds, err := cfgBuilder.BuildDatasource()
	if err != nil {
		return err
	}
	p.hdl = newHandler(ds, algorithm)
	return nil
}

func (p *Plugin) Join(next plugin.Handler) plugin.Handler {
	return p.hdl
}
