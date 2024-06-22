package log

import (
	"database/sql/driver"
	"log/slog"
)

func NewConnector(d driver.Driver, dsn string, l *slog.Logger) (driver.Connector, error) {
	return newDriver(d, newLogger(l)).OpenConnector(dsn)
}
