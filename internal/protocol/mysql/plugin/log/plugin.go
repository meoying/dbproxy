package log

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

type Plugin struct {
	log *slog.Logger
}

func (p *Plugin) Name() string {
	return "log"
}

func (p *Plugin) Init(cfg []byte) error {
	var c Config
	err := json.Unmarshal(cfg, &c)
	if err != nil {
		return err
	}
	err = os.MkdirAll(filepath.Dir(c.Output), os.ModePerm)
	if err != nil {
		return err
	}
	p.log, err = initLogger(c)
	if err != nil {
		return err
	}
	return nil
}

func (p *Plugin) Join(next plugin.Handler) plugin.Handler {
	return plugin.HandleFunc(func(ctx *pcontext.Context) (*plugin.Result, error) {
		p.log.Debug("处理SQL语句：", "SQL", ctx.Query)
		return next.Handle(ctx)
	})
}

// initLogger 根据配置初始化日志处理器
func initLogger(config Config) (*slog.Logger, error) {
	// var level slog.Level
	// switch config.Level {
	// case "debug":
	// 	level = slog.LevelDebug
	// case "info":
	// 	level = slog.LevelInfo
	// case "error":
	// 	level = slog.LevelError
	// default:
	// 	level = slog.LevelInfo
	// }
	var output io.Writer
	switch config.Output {
	case "stdout":
		output = os.Stdout
	case "stderr":
		output = os.Stderr
	default:
		file, err := os.Create(config.Output)
		if err != nil {
			return nil, err
		}
		output = file
	}
	return slog.New(slog.NewTextHandler(output, nil)), nil
}
