package log

import (
	"database/sql"
	"log/slog"
	"os"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
)

func Open(name string) (*sql.DB, error) {
	d := &mysql.MySQLDriver{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	wrappedDriver := newDriver(d, NewSLogger(logger))
	connector, err := wrappedDriver.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(connector), nil
}
