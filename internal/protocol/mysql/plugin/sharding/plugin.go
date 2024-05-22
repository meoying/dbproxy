package sharding

import (
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	pcontext "github.com/meoying/dbproxy/internal/protocol/mysql/plugin/context"
	"github.com/meoying/dbproxy/internal/sharding"
)

type Plugin struct {
	ds        datasource.DataSource
	algorithm sharding.Algorithm
}

func (p *Plugin) Name() string {
	return "sharding"
}

func (p *Plugin) Init(cfg []byte) error {
	// 在这里初始化 p.ds
	// 初始化分库分表的规则，目前你可以认为只支持哈希类的
	// p.ds = shardingsource.NewShardingDataSource()
	// p.algorithm = hash.Hash{}
	return nil
}

func (p *Plugin) Join(next plugin.Handler) plugin.Handler {
	return plugin.HandleFunc(func(ctx *pcontext.Context) (*plugin.Result, error) {
		// 要完成几个步骤：
		// 1. 从 ctx.ParsedQuery 里面拿到 Where 部分，参考 ast 里面的东西来看怎么拿 WHERE
		// 如果是 INSERT，则是拿到 VALUE 或者 VALUES 的部分
		// 2. 用 1 步骤的结果，调用 p.algorithm 拿到分库分表的结果
		// 3. 调用 p.ds.Exec 或者 p.ds.Query
		panic("implement me")
	})
}
