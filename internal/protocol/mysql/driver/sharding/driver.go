package sharding

import (
	"database/sql/driver"

	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/sharding"
)

type Driver struct {
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	// TODO implement me
	panic("implement me")
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	// TODO implement me
	panic("implement me")
}

func NewConnector(ds datasource.DataSource, algorithm sharding.Algorithm) (driver.Connector, error) {
	return newConnector(ds, algorithm), nil
}
