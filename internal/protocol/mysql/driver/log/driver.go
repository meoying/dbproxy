package log

import (
	"database/sql/driver"

	driver2 "github.com/meoying/dbproxy/internal/protocol/mysql/driver"
)

var _ driver2.Driver = &driverWrapper{}

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

func (d *driverWrapper) Open(name string) (driver.Conn, error) {
	conn, err := d.driver.Open(name)
	if err != nil {
		d.logger.Error("打开连接失败", "名称", name, "错误", err)
		return nil, err
	}
	d.logger.Info("打开连接成功", "名称", name)
	return &connWrapper{conn: conn, logger: d.logger}, nil
}

func (d *driverWrapper) OpenConnector(name string) (driver.Connector, error) {
	openConnector, err := d.driver.(driver.DriverContext).OpenConnector(name)
	if err != nil {
		d.logger.Error("打开连接器失败", "名称", name, "错误", err)
		return nil, err
	}
	d.logger.Info("连接器打开成功", "名称", name)
	return &connectorWrapper{connector: openConnector, driver: d.driver, logger: d.logger}, nil
}
