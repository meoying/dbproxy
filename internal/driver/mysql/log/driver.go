package log

import (
	"database/sql/driver"
)

type driverWrapper struct {
	driver driver.Driver
	logger logger
}

func newDriver(d driver.Driver, l logger) *driverWrapper {
	return &driverWrapper{
		driver: d,
		logger: l,
	}
}

func (d *driverWrapper) OpenConnector(name string) (driver.Connector, error) {
	openConnector, err := d.driver.(driver.DriverContext).OpenConnector(name)
	if err != nil {
		d.logger.Error("open connector failed", "name", name, "error", err)
		return nil, err
	}
	d.logger.Info("connector opened successfully", "name", name)
	return &connectorWrapper{connector: openConnector, driver: d.driver, logger: d.logger}, nil
}
