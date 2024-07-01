package sharding

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/go-sql-driver/mysql"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/cluster"
	"github.com/meoying/dbproxy/internal/datasource/masterslave"
	"github.com/meoying/dbproxy/internal/datasource/shardingsource"
	logdriver "github.com/meoying/dbproxy/internal/protocol/mysql/driver/log"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	shardinghandler "github.com/meoying/dbproxy/internal/protocol/mysql/internal/sharding"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/vparser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	"github.com/meoying/dbproxy/internal/sharding"
	"github.com/meoying/dbproxy/internal/sharding/hash"
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
	// 从配置文件加载ds 和 algorithm
	var shardingCfg Config
	err := json.Unmarshal(cfg, &shardingCfg)
	if err != nil {
		return fmt.Errorf("解析配置文件失败 %w", err)
	}
	return p.initDemo(shardingCfg)
}

// 初始化demo的配置
func (p *Plugin) initDemo(cfg Config) error {
	shardAlgorithm := &hash.Hash{
		ShardingKey: cfg.ShardingKey,
	}
	if cfg.DBBase != 0 {
		shardAlgorithm.DBPattern = &hash.Pattern{Name: cfg.DBPattern, Base: cfg.DBBase}
	} else {
		shardAlgorithm.DBPattern = &hash.Pattern{Name: cfg.DBPattern, NotSharding: true}
	}
	if cfg.TableBase != 0 {
		shardAlgorithm.TablePattern = &hash.Pattern{Name: cfg.TablePattern, Base: cfg.TableBase}
	} else {
		shardAlgorithm.TablePattern = &hash.Pattern{Name: cfg.TablePattern, NotSharding: true}
	}
	if cfg.DSBase != 0 {
		shardAlgorithm.DsPattern = &hash.Pattern{Name: cfg.DSPattern, Base: cfg.DSBase}
	} else {
		shardAlgorithm.DsPattern = &hash.Pattern{Name: cfg.DSPattern, NotSharding: true}
	}
	log.Println(cfg)
	// demo只分库
	m := map[string]*masterslave.MasterSlavesDB{}
	if len(cfg.DBDsns) < cfg.DBBase {
		return fmt.Errorf("配置文件设置错误")
	}
	for i := 0; i < cfg.DBBase; i++ {
		dbname := fmt.Sprintf(cfg.DBPattern, i)
		db, err := openDB(cfg.DBDsns[i])
		if err != nil {
			return err
		}
		m[dbname] = masterslave.NewMasterSlavesDB(db)
	}
	clusterDB := cluster.NewClusterDB(m)
	ds := map[string]datasource.DataSource{
		cfg.DSPattern: clusterDB,
	}
	dss := shardingsource.NewShardingDataSource(ds)
	p.ds = dss
	p.algorithm = shardAlgorithm
	p.handlerMap = map[string]shardinghandler.NewHandlerFunc{
		vparser.SelectSql: shardinghandler.NewSelectHandler,
		vparser.InsertSql: shardinghandler.NewInsertBuilder,
		vparser.UpdateSql: shardinghandler.NewUpdateHandler,
		vparser.DeleteSql: shardinghandler.NewDeleteHandler,
	}
	return nil
}

func openDB(dsn string) (*sql.DB, error) {
	l := slog.New(slog.NewTextHandler(os.Stdout, nil))
	connector, err := logdriver.NewConnector(&mysql.MySQLDriver{}, dsn, logdriver.WithLogger(l))
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(connector), nil
}

func NewPlugin(ds datasource.DataSource, algorithm sharding.Algorithm) *Plugin {

	return &Plugin{
		ds:        ds,
		algorithm: algorithm,
		handlerMap: map[string]shardinghandler.NewHandlerFunc{
			vparser.SelectSql: shardinghandler.NewSelectHandler,
			vparser.InsertSql: shardinghandler.NewInsertBuilder,
			vparser.UpdateSql: shardinghandler.NewUpdateHandler,
			vparser.DeleteSql: shardinghandler.NewDeleteHandler,
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
