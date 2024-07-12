package forward

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"os"

	"github.com/go-sql-driver/mysql"
	"github.com/meoying/dbproxy/internal/datasource/single"
	logdriver "github.com/meoying/dbproxy/internal/protocol/mysql/driver/log"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

var _ plugin.Plugin = &Plugin{}

type Plugin struct {
	hdl *Handler
}

func (p *Plugin) Name() string {
	return "forward"
}

func (p *Plugin) Init(cfg []byte) error {
	var config Config
	err := json.Unmarshal(cfg, &config)
	if err != nil {
		return err
	}
	db, err := openDB(config.Dsn)
	if err != nil {
		return err
	}
	// TODO 这里是否要支持主从?还是单个?也就是说确定配置具体内容
	p.hdl = NewHandler(single.NewDB(db))
	return nil
}

func openDB(dsn string) (*sql.DB, error) {
	l := slog.New(slog.NewTextHandler(os.Stdout, nil))
	connector, err := logdriver.NewConnector(&mysql.MySQLDriver{}, dsn, logdriver.WithLogger(l))
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(connector), nil
}

func (p *Plugin) Join(next plugin.Handler) plugin.Handler {
	return p.hdl
}
