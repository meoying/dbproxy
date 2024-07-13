package sharding

import (
	"encoding/json"
	"log"

	shardingconfig "github.com/meoying/dbproxy/config/mysql/sharding"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/protocol/mysql/configbuilder"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	shardinghandler "github.com/meoying/dbproxy/internal/protocol/mysql/internal/sharding"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/vparser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	"github.com/meoying/dbproxy/internal/sharding"
)

type Plugin struct {
	ds         datasource.DataSource
	algorithm  sharding.Algorithm
	handlerMap map[string]shardinghandler.NewHandlerFunc
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
	pp := NewPlugin(ds, algorithm)
	*p = *pp
	return nil
}

func NewPlugin(ds datasource.DataSource, algorithm sharding.Algorithm) *Plugin {
	return &Plugin{
		ds:        ds,
		algorithm: algorithm,
		handlerMap: map[string]shardinghandler.NewHandlerFunc{
			vparser.SelectStmt: shardinghandler.NewSelectHandler,
			vparser.InsertStmt: shardinghandler.NewInsertBuilder,
			vparser.UpdateStmt: shardinghandler.NewUpdateHandler,
			vparser.DeleteStmt: shardinghandler.NewDeleteHandler,
		},
	}
}

func (p *Plugin) Join(next plugin.Handler) plugin.Handler {
	return plugin.HandleFunc(func(ctx *pcontext.Context) (*plugin.Result, error) {
		// 要完成几个步骤：
		// 1. 从 ctx.ParsedQuery 里面拿到 Where 部分，参考 ast 里面的东西来看怎么拿 WHERE
		// 如果是 INSERT，则是拿到 VALUE 或者 VALUES 的部分
		// 2. 用 1 步骤的结果，调用 p.algorithm 拿到分库分表的结果
		// 3. 调用 p.ds.Exec 或者 p.ds.Query
		log.Println("xxxxxx dasndosandosa")
		if next != nil {
			_, _ = next.Handle(ctx)
		}
		defer func() {
			if r := recover(); r != nil {
				log.Println("分库分表查询失败")
			}
		}()
		checkVisitor := vparser.NewCheckVisitor()
		sqlName := checkVisitor.Visit(ctx.ParsedQuery.Root).(string)
		newHandlerFunc, ok := p.handlerMap[sqlName]
		if !ok {
			log.Printf("YYYYYYY sqlName = %#v, handlerMap = %#v\n", sqlName, p.handlerMap)
			return nil, shardinghandler.ErrUnKnowSql
		}

		handler, err := newHandlerFunc(p.algorithm, p.ds, ctx)
		if err != nil {
			return nil, err
		}
		r, err := handler.QueryOrExec(ctx.Context)
		if err != nil {
			return nil, err
		}
		return (*plugin.Result)(r), nil
	})
}
