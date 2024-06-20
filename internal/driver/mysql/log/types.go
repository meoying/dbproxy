package log

import (
	"log/slog"
)

//go:generate mockgen -source=./types.go -destination=mocks/logger.mock.go -package=logmocks -typed Logger
type Logger interface {
	Logf(format string, args ...any)
	Errorf(format string, args ...any)
}

type slogWrapper struct {
	slogger *slog.Logger
}

func NewSLogger(l *slog.Logger) Logger {
	return &slogWrapper{slogger: l}
}

func (l *slogWrapper) Logf(format string, args ...any) {
	l.slogger.Info(format, args...)
}

func (l *slogWrapper) Errorf(format string, args ...any) {
	l.slogger.Error(format, args...)
}
