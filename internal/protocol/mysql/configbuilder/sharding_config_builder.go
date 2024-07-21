package configbuilder

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	"github.com/go-sql-driver/mysql"
	shardingconfig "github.com/meoying/dbproxy/config/mysql/plugin/sharding"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/cluster"
	"github.com/meoying/dbproxy/internal/datasource/masterslave"
	"github.com/meoying/dbproxy/internal/datasource/shardingsource"
	logdriver "github.com/meoying/dbproxy/internal/protocol/mysql/driver/log"
	"github.com/meoying/dbproxy/internal/sharding"
	"github.com/meoying/dbproxy/internal/sharding/hash"
	"github.com/spf13/viper"
)

// ShardingConfigBuilder 根据配置信息构建Algorithm对象和Datasource对象
type ShardingConfigBuilder struct {
	config *shardingconfig.Config
}

// LoadConfigFile 根据绝对路径path加载配置文件
func (s *ShardingConfigBuilder) LoadConfigFile(path string) error {
	viper.SetConfigType("yaml")
	viper.SetConfigFile(path)
	err := viper.ReadInConfig()
	if err != nil {
		return fmt.Errorf("读取配置文件失败: %w", err)
	}
	var cfg shardingconfig.Config
	err = viper.Unmarshal(&cfg)
	if err != nil {
		return fmt.Errorf("解析配置文件失败: %w", err)
	}
	s.SetConfig(cfg)
	return nil
}

func (s *ShardingConfigBuilder) SetConfig(cfg shardingconfig.Config) {
	s.config = &cfg
}

func (s *ShardingConfigBuilder) Config() shardingconfig.Config {
	return *s.config
}

func (s *ShardingConfigBuilder) BuildAlgorithm() (sharding.Algorithm, error) {
	if err := s.checkConfig(); err != nil {
		return nil, err
	}
	if s.config.Algorithm.Hash == nil {
		return nil, fmt.Errorf("未配置分片算法")
	}
	h := s.config.Algorithm.Hash
	return &hash.Hash{
		ShardingKey:  h.ShardingKey,
		DsPattern:    &hash.Pattern{Base: h.DSPattern.Base, Name: h.DSPattern.Name, NotSharding: h.DSPattern.NotSharding},
		DBPattern:    &hash.Pattern{Base: h.DBPattern.Base, Name: h.DBPattern.Name, NotSharding: h.DBPattern.NotSharding},
		TablePattern: &hash.Pattern{Base: h.TBPattern.Base, Name: h.TBPattern.Name, NotSharding: h.TBPattern.NotSharding},
	}, nil
}

func (s *ShardingConfigBuilder) checkConfig() error {
	if s.config == nil {
		return fmt.Errorf("未加载或设置配置文件")
	}
	return nil
}

func (s *ShardingConfigBuilder) BuildDatasource() (datasource.DataSource, error) {
	if err := s.checkConfig(); err != nil {
		return nil, err
	}
	clusters := make(map[string]datasource.DataSource)
	for _, clusterCfg := range s.config.Datasource.Clusters {
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
