package sharding

import (
	"context"
	"database/sql/driver"

	"github.com/meoying/dbproxy/internal/datasource"
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

// Driver
// TODO: 如果不可用则实现有破口不是完整语义, 如果实现则Driver上的Open和OpenConnector方法在分库分表的语境下是什么语义? 允许动态添加数据库实例?
func (c *connector) Driver() driver.Driver {
	// TODO implement me
	panic("implement me")
}
