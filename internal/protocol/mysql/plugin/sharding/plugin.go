package sharding

import (
	"encoding/json"

	shardingconfig "github.com/meoying/dbproxy/config/mysql/plugin/sharding"
	"github.com/meoying/dbproxy/internal/protocol/mysql/configbuilder"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/internal/handler"
)

type Plugin struct {
	hdl *handler.ShardingHandler
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
	p.hdl = handler.NewShardingHandler(ds, algorithm)
	return nil
}

func (p *Plugin) Join(next plugin.Handler) plugin.Handler {
	return p.hdl
}
