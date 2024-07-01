package sharding

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"os"

	"github.com/go-sql-driver/mysql"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/cluster"
	"github.com/meoying/dbproxy/internal/datasource/masterslave"
	"github.com/meoying/dbproxy/internal/datasource/shardingsource"
	logdriver "github.com/meoying/dbproxy/internal/protocol/mysql/driver/log"
	"github.com/meoying/dbproxy/internal/sharding"
	"github.com/meoying/dbproxy/internal/sharding/hash"
	"gopkg.in/yaml.v3"
)

type connector struct {
	ds        datasource.DataSource
	algorithm sharding.Algorithm
}

func newConnector(ds datasource.DataSource, algorithm sharding.Algorithm) *connector {
	return &connector{ds: ds, algorithm: algorithm}
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	return newConnection(c.ds, c.algorithm), nil
}

func (c *connector) Driver() driver.Driver {
	return &driverImpl{}
}

// ConnectorBuilder 根据配置信息构建driver.Connector对象或者*sql.DB对象
type ConnectorBuilder struct {
	config *Config
}

func (c *ConnectorBuilder) LoadConfigFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("读取配置文件失败: %w", err)
	}
	var cfg Config
	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return fmt.Errorf("解析配置文件失败: %w", err)
	}
	c.SetConfig(cfg)
	return nil
}

func (c *ConnectorBuilder) SetConfig(cfg Config) {
	cc := cfg
	c.config = &cc
}

// BuildDB 根据配置文件直接构建出*sql.DB对象
func (c *ConnectorBuilder) BuildDB() (*sql.DB, error) {
	cc, err := c.Build()
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(cc), nil
}

// Build 根据配置文件构建出Connector对象
func (c *ConnectorBuilder) Build() (driver.Connector, error) {
	if c.config == nil {
		return nil, fmt.Errorf("未设置配置信息")
	}
	h, err := c.hashAlgorithm()
	if err != nil {
		return nil, err
	}
	d, err := c.datasource()
	if err != nil {
		return nil, err
	}
	return newConnector(d, h), nil
}

func (c *ConnectorBuilder) hashAlgorithm() (*hash.Hash, error) {
	if c.config.Algorithm.Hash == nil {
		return nil, fmt.Errorf("未配置分片算法")
	}
	h := c.config.Algorithm.Hash
	return &hash.Hash{
		ShardingKey:  h.ShardingKey,
		DsPattern:    &hash.Pattern{Base: h.DSPattern.Base, Name: h.DSPattern.Name, NotSharding: h.DSPattern.NotSharding},
		DBPattern:    &hash.Pattern{Base: h.DBPattern.Base, Name: h.DBPattern.Name, NotSharding: h.DBPattern.NotSharding},
		TablePattern: &hash.Pattern{Base: h.TBPattern.Base, Name: h.TBPattern.Name, NotSharding: h.TBPattern.NotSharding},
	}, nil
}

func (c *ConnectorBuilder) datasource() (datasource.DataSource, error) {
	clusters := make(map[string]datasource.DataSource)
	for _, clusterCfg := range c.config.Datasource.Clusters {
		clusterNodes := make(map[string]*masterslave.MasterSlavesDB, len(clusterCfg.Nodes))
		for _, node := range clusterCfg.Nodes {
			db, err := openDB(node.Master.DSN)
			if err != nil {
				return nil, err
			}
			// TODO: 初始化从节点
			clusterNodes[node.Master.Name] = masterslave.NewMasterSlavesDB(db)
		}
		clusters[clusterCfg.Address] = cluster.NewClusterDB(clusterNodes)
	}
	return shardingsource.NewShardingDataSource(clusters), nil
}

func openDB(dsn string) (*sql.DB, error) {
	l := slog.New(slog.NewTextHandler(os.Stdout, nil))
	c, err := logdriver.NewConnector(&mysql.MySQLDriver{}, dsn, logdriver.WithLogger(l))
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(c), nil
}
