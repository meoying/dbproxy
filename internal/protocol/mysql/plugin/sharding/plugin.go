package sharding

import (
	"errors"
	"fmt"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/masterslave"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	pcontext "github.com/meoying/dbproxy/internal/protocol/mysql/plugin/context"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor"
	"github.com/meoying/dbproxy/internal/sharding"
	"log"
	"strings"
)

type Plugin struct {
	ds        datasource.DataSource
	algorithm sharding.Algorithm
}

func NewPlugin(ds datasource.DataSource, algorithm sharding.Algorithm) *Plugin {
	return &Plugin{
		ds:        ds,
		algorithm: algorithm,
	}
}

func (p *Plugin) NewVisitor() map[string]visitor.Visitor {
	visitors := []visitor.Visitor{
		visitor.NewInsertVisitor(),
		visitor.NewsSelectVisitor(),
		visitor.NewCheckVisitor(),
		visitor.NewHintVisitor(),
	}
	visitorMap := make(map[string]visitor.Visitor, 16)
	for _, v := range visitors {
		visitorMap[p.getVisitorName(v)] = v
	}
	return visitorMap
}
func (p *Plugin) getVisitorName(v visitor.Visitor) string {
	return fmt.Sprintf("sharding_%s", v.Name())
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
		if next != nil {
			next.Handle(ctx)
		}
		defer func() {
			if r := recover(); r != nil {
				log.Println("分库分表查询失败")
			}
		}()
		checkVisitor, ok := ctx.Visitors["sharding_CheckVisitor"]
		if !ok {
			return nil, errors.New("缺少checkVisitor")
		}
		nameResp := checkVisitor.VisitRoot(ctx.ParsedQuery.Root.(*parser.RootContext))
		switch nameResp.(string) {
		case visitor.InsertSql:
			handler, err := NewInsertBuilder(p.algorithm, p.ds, ctx)
			if err != nil {
				return nil, err
			}
			res := handler.Exec(ctx.Context)
			if res.Err() != nil {
				return nil, res.Err()
			}
			return &plugin.Result{
				Result: res,
			}, nil
		case visitor.SelectSql:
			hintVisit := ctx.Visitors["sharding_HintVisitor"]
			hint := hintVisit.Visit(ctx.ParsedQuery.Root)

			if strings.Contains(hint.(string), "useMaster") {
				qctx := masterslave.UseMaster(ctx.Context)
				ctx.Context = qctx
			}
			handler, err := NewSelectHandler(p.algorithm, p.ds, ctx)
			if err != nil {
				return nil, err
			}
			res, err := handler.GetMulti(ctx.Context)
			return &plugin.Result{
				Rows: res,
			}, err
		default:
			return nil, errors.New("未知语句")
		}

	})
}
