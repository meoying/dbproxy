package log

import (
	"database/sql/driver"
)

type driverWrapper struct {
	driver driver.Driver
	logger Logger
}

func newDriver(d driver.Driver, l Logger) *driverWrapper {
	return &driverWrapper{
		driver: d,
		logger: l,
	}
}

func (d *driverWrapper) Open(name string) (driver.Conn, error) {
	con, err := d.driver.Open(name)
	if err != nil {
		d.logger.Errorf("Failed to Open %s: %v", name, err)
		return nil, err
	}
	d.logger.Logf("Open")
	return &connWrapper{conn: con, logger: d.logger}, nil
}

func (d *driverWrapper) OpenConnector(name string) (driver.Connector, error) {
	openConnector, err := d.driver.(driver.DriverContext).OpenConnector(name)
	if err != nil {
		d.logger.Errorf("Failed to OpenConnector for %s: %v", name, err)
		return nil, err
	}
	d.logger.Logf("OpenConnector")
	return &connectorWrapper{connector: openConnector, driver: d.driver, logger: d.logger}, nil
}
