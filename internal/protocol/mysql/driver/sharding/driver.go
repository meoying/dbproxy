package sharding

import (
	"database/sql/driver"
)

type driverImpl struct {
}

func (d *driverImpl) Open(name string) (driver.Conn, error) {
	panic("暂不支持,有需要可以提issue")
}

func (d *driverImpl) OpenConnector(name string) (driver.Connector, error) {
	panic("暂不支持,有需要可以提issue")
}
