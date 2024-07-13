package sharding

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/protocol/mysql/configbuilder"
	"github.com/meoying/dbproxy/internal/sharding"
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
	configbuilder.ShardingConfigBuilder
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
	h, err := c.ShardingConfigBuilder.BuildAlgorithm()
	if err != nil {
		return nil, err
	}
	d, err := c.ShardingConfigBuilder.BuildDatasource()
	if err != nil {
		return nil, err
	}
	return newConnector(d, h), nil
}
