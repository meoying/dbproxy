package log

import (
	"database/sql/driver"
	"log/slog"
)

//go:generate mockgen -source=./types.go -destination=mocks/logger.mock.go -package=logmocks -typed logger
type logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

type ConnectorOptions struct {
	l *slog.Logger
}

type Option func(*ConnectorOptions)

func WithLogger(l *slog.Logger) Option {
	return func(opts *ConnectorOptions) {
		opts.l = l
	}
}

func NewConnector(d driver.Driver, dsn string, opts ...Option) (driver.Connector, error) {
	options := &ConnectorOptions{
		l: slog.Default(),
	}
	for _, opt := range opts {
		opt(options)
	}
	return newDriver(d, options.l).OpenConnector(dsn)
}
