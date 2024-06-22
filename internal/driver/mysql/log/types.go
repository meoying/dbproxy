package log

import (
	"fmt"
	"log/slog"
	"os"
)

//go:generate mockgen -source=./types.go -destination=mocks/logger.mock.go -package=logmocks -typed Logger
type Logger interface {
	Logf(format string, args ...any)
	Errorf(format string, args ...any)
}

type defaultWrapper struct {
	slogger *slog.Logger
}

func newDefaultLogger() Logger {
	return &defaultWrapper{slogger: slog.New(slog.NewTextHandler(os.Stdout, nil))}
}

func (l *defaultWrapper) Logf(format string, args ...any) {
	l.slogger.Info(fmt.Sprintf(format, args...))
}

func (l *defaultWrapper) Errorf(format string, args ...any) {
	l.slogger.Error(fmt.Sprintf(format, args...))
}
