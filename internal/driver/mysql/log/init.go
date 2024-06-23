package log

import (
	"database/sql/driver"
	"log/slog"
)

func NewConnector(d driver.Driver, dsn string, l *slog.Logger) (driver.Connector, error) {
	if l == nil {
		l = slog.Default()
	}
	return newDriver(d, l).OpenConnector(dsn)
}
