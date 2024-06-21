package log

import (
	"context"
	"database/sql/driver"
)

type connectorWrapper struct {
	connector driver.Connector
	driver    driver.Driver
	logger    Logger
}

func (c *connectorWrapper) Connect(ctx context.Context) (driver.Conn, error) {
	con, err := c.connector.Connect(ctx)
	if err != nil {
		c.logger.Errorf("Failed to Connect: %v", err)
		return nil, err
	}
	c.logger.Logf("Connect\n")
	return &connWrapper{conn: con, logger: c.logger}, err
}

func (c *connectorWrapper) Driver() driver.Driver {
	c.logger.Logf("Driver\n")
	return c.driver
}
