package log

import (
	"context"
	"log/slog"
)

//go:generate mockgen -source=./types.go -destination=mocks/logger.mock.go -package=logmocks -typed Logger
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)

	DebugContext(ctx context.Context, msg string, args ...any)
	InfoContext(ctx context.Context, msg string, args ...any)
	WarnContext(ctx context.Context, msg string, args ...any)
	ErrorContext(ctx context.Context, msg string, args ...any)

	Enabled(ctx context.Context, level slog.Level) bool

	With(args ...any) *slog.Logger
	WithGroup(name string) *slog.Logger

	Handler() slog.Handler
}

type defaultLogger struct {
	*slog.Logger
}

func newLogger(l *slog.Logger) Logger {
	if l == nil {
		l = slog.Default()
	}
	return &defaultLogger{
		Logger: l,
	}
}
