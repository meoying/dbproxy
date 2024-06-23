package log

import (
	"context"
	"database/sql/driver"
)

type connectorWrapper struct {
	connector driver.Connector
	driver    driver.Driver
	logger    logger
}

func (c *connectorWrapper) Connect(ctx context.Context) (driver.Conn, error) {
	con, err := c.connector.Connect(ctx)
	if err != nil {
		c.logger.Error("建立连接失败", "错误", err)
		return nil, err
	}
	c.logger.Info("连接建立成功")
	return &connWrapper{conn: con, logger: c.logger}, nil
}

func (c *connectorWrapper) Driver() driver.Driver {
	c.logger.Info("请求获取驱动")
	return c.driver
}
