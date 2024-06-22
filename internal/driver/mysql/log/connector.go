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
		c.logger.Error("connection establishment failed", "error", err)
		return nil, err
	}
	c.logger.Info("connection established successfully")
	return &connWrapper{conn: con, logger: c.logger}, nil
}

func (c *connectorWrapper) Driver() driver.Driver {
	c.logger.Info("driver requested")
	return c.driver
}
